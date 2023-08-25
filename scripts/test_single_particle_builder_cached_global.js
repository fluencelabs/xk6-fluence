import fluence from "k6/x/fluence";

// Run with ./k6 run ./scripts/test_single_particle_builder_cached_global.js -i 100 --vus 10

export default function () {
    let relay =
        "/ip4/127.0.0.1/tcp/9999/ws/p2p/12D3KooWEmP9NHMmZeGLMhx5CtXyLS9VRhtQ7CWcXERgjMyTe1Vc";
    let script = `
    (xor
        (seq
            (call %init_peer_id% ("load" "relay") [] init_relay)
            (seq
                (call init_relay ("op" "identity") ["hello world!"] result)
                (call %init_peer_id% ("callback" "callback") [result])
            )
        )
        (seq
            (call init_relay ("op" "identity") [])
            (call %init_peer_id% ("callback" "error") [%last_error%])
        )
    )`;

    let connection = null;
    try {
        let builder = fluence.builder(relay);
        connection = builder.cacheBy(1).connect();
        connection.send(script);
    } finally {
        if (connection != null) connection.close();
    }
}
