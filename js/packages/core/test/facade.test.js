const { describe, it } = require('node:test');
const assert = require('node:assert');
const { build, execute } = require('../src/facade.js');
const { hydrate } = require('../src/hydrate.js');
const { parse } = require('../src/parse.js');

async function readAll(stream) {
  const reader = stream.getReader();
  const chunks = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks).toString('utf8');
}

describe('facade build/execute', () => {
  it('build() returns ir + collected map for a simple request', async () => {
    const { ir, map } = await build('POST /u/{{ id | url }} HTTP/1.1\nHost: api.test\n', { id: 'a b' });
    assert.equal(ir.method, 'POST');
    assert.equal(ir.uri, '/u/a%20b');
    assert.equal(ir.host, 'api.test');
    assert.equal(map.length, 1);
    assert.deepEqual(map[0], {
      'hydrated-start': 8,
      'original-start': 8,
      'hydrated-length': 5, // 'a b' -> url-encoded 'a%20b'
      'original-length': 14, // '{{ id | url }}'
    });
  });

  // Regression: a worker error fails BOTH the resolved and map channels. build()
  // must reject cleanly without orphaning the map-stream rejection (which would
  // surface as an unhandledRejection and abort the process).
  it('build() rejects cleanly on BodyConflictError with no unhandled rejection', async () => {
    const seen = [];
    const onUnhandled = (err) => seen.push(err);
    process.on('unhandledRejection', onUnhandled);
    try {
      await assert.rejects(
        () => build('POST / HTTP/1.1\nHost: x\n\nbody', { body: { type: 'text', content: 'x' } }),
        (err) => err.name === 'BodyConflictError'
      );
      await new Promise((r) => setTimeout(r, 20));
      assert.deepEqual(seen, [], 'no unhandled rejection should escape build()');
    } finally {
      process.removeListener('unhandledRejection', onUnhandled);
    }
  });

  it('build() rejects with TemplateSyntaxError on an unknown filter', async () => {
    await assert.rejects(
      () => build('GET /{{ x | bogus }} HTTP/1.1\n', { x: 1 }),
      (err) => err.name === 'TemplateSyntaxError'
    );
  });

  // Regression: out-of-band body sourced from streams[] must be delivered.
  it('build() delivers a provided body from the streams array', async () => {
    const { ir, bodyStream } = await build(
      'PUT /up HTTP/1.1\nHost: api.test\n:httpt-body-type: provided\n',
      {},
      ['PROVIDED-PAYLOAD']
    );
    assert.deepEqual(ir.body, { type: 'provided', content: 0 });
    assert.equal(await readAll(bodyStream), 'PROVIDED-PAYLOAD');
  });
});

describe('parse pseudo-headers', () => {
  // Regression: ":authority" must lift to host, not become a garbage empty-name header.
  it('lifts the :authority pseudo-header into host', async () => {
    const { resolvedStream, bodyStream } = hydrate(
      'GET /p HTTP/1.1\n:authority: api.example.com\nAccept: */*\n',
      {}
    );
    const { ir } = await parse(resolvedStream, bodyStream);
    assert.equal(ir.host, 'api.example.com');
    assert.deepEqual(ir.headers, [{ name: 'Accept', value: '*/*' }]);
  });
});
