In the E2E tests, instead of using `#[cfg_attr(![...], ignore)]`, which still compiles the ignored tests, use `#[cfg([...])]`.
