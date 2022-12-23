use std::str::FromStr;

use fancy_regex::Regex;
use schemars::JsonSchema;
use serde::Deserialize;
use thiserror::Error;

use crate::{
    config::{
        from_env::FromEnv, source::MirrordConfigSource, ConfigError, FromMirrordConfig,
        MirrordConfig, Result,
    },
    util::MirrordToggleableConfig,
};

#[derive(Deserialize, PartialEq, Eq, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum IncomingConfig {
    #[default]
    Mirror,
    Steal(StealConfig),
}

#[derive(Deserialize, PartialEq, Eq, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StealConfig {
    #[default]
    All,
    HttpHeaderFilter(String),
}

impl FromMirrordConfig for IncomingConfig {
    type Generator = IncomingConfig;
}

impl MirrordToggleableConfig for IncomingConfig {
    fn disabled_config() -> Result<Self::Generated, ConfigError> {
        let filter: Option<String> = FromEnv::new("MIRRORD_HTTP_HEADER_FILTER")
            .source_value()
            .transpose()?;
        if let Some(filter) = filter {
            Regex::new(&filter)?;
            // if http is set, steal is set.
            Ok(IncomingConfig::Steal(StealConfig::HttpHeaderFilter(filter)))
        } else {
            FromEnv::new("MIRRORD_AGENT_TCP_STEAL_TRAFFIC")
                .source_value()
                .unwrap_or(Ok(IncomingConfig::Mirror))
        }
    }
}

impl MirrordConfig for StealConfig {
    type Generated = StealConfig;
    fn generate_config(self) -> Result<Self::Generated> {
        match &self {
            StealConfig::All => Ok(self),
            StealConfig::HttpHeaderFilter(regex) => {
                Regex::new(regex)?; // Raise error if regex does not compile.
                Ok(self)
            }
        }
    }
}

impl MirrordConfig for IncomingConfig {
    type Generated = IncomingConfig;

    fn generate_config(self) -> Result<Self::Generated> {
        Ok(self)
    }
}

#[derive(Error, Debug)]
#[error("could not parse IncomingConfig from string, values must be bool or mirror/steal")]
pub struct IncomingConfigParseError;

impl FromStr for IncomingConfig {
    type Err = IncomingConfigParseError;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val.parse::<bool>() {
            Ok(true) => Ok(IncomingConfig::Steal(StealConfig::All)),
            Ok(false) => Ok(IncomingConfig::Mirror),
            Err(_) => match val {
                "steal" => Ok(IncomingConfig::Steal(StealConfig::All)),
                "mirror" => Ok(IncomingConfig::Mirror),
                _ => Err(IncomingConfigParseError),
            },
        }
    }
}

impl IncomingConfig {
    pub fn is_steal(&self) -> bool {
        matches!(self, &IncomingConfig::Steal(_))
    }

    pub fn get_http_filter(&self) -> Option<String> {
        match self {
            IncomingConfig::Steal(StealConfig::HttpHeaderFilter(filter)) => {
                Some(filter.to_string())
            }
            _ => None,
        }
    }
}
