import dgram from 'node:dgram';

const server = dgram.createSocket('udp4');

server.on('error', (err) => {
    console.log(`server error:\n${err.stack}`);
    server.close();
    throw err
});

server.on('message', (msg, rinfo) => {
    console.log(`server got: ${msg} from ${rinfo.address}:${rinfo.port}`);

    server.send('Can I pass the test please?\n', rinfo.port, rinfo.address, function(error){
        if(error) {
            server.close();
            throw error
        }
        server.close();
    });
});

server.on('listening', () => {
    const address = server.address();
    console.log(`server listening ${address.address}:${address.port}`);
});

server.bind(31415);
// Prints: server listening 0.0.0.0:80
