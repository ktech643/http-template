const fs = require('node:fs');
const path = require('node:path');
const { describe, it } = require('node:test');
const assert = require('node:assert');
const { hydrate, parse } = require('../src/pipeline.js');
const { loadE2eFixtures, E2E_DIR } = require('@httpt/test-utils');

/** Collects an async iterable into an array. */
async function collect(iterable) {
  const out = [];
  for await (const item of iterable) out.push(item);
  return out;
}

/** Fully reads a ReadableStream<Uint8Array> into a utf8 string. */
async function readBody(stream) {
  const reader = stream.getReader();
  const chunks = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks).toString('utf8');
}

/** Consumes all three hydrate outputs. */
async function hydrateAll(template, data) {
  const { resolvedStream, mapStream, bodyStream } = hydrate(template, data);
  const [head, map, body] = await Promise.all([
    (async () => (await collect(resolvedStream)).join(''))(),
    collect(mapStream),
    readBody(bodyStream),
  ]);
  return { head, map, body };
}

/** Splits a hydrated request at the first head/body boundary. */
function splitBoundary(s) {
  const lf = s.indexOf('\n\n');
  const crlf = s.indexOf('\r\n\r\n');
  let idx = -1;
  let len = 0;
  if (crlf !== -1 && (lf === -1 || crlf <= lf)) {
    idx = crlf;
    len = 4;
  } else if (lf !== -1) {
    idx = lf;
    len = 2;
  }
  if (idx === -1) return { head: s, body: null };
  return { head: s.slice(0, idx), body: s.slice(idx + len) };
}

describe('Pipeline: Hydrate & Parse', () => {
  const fixtures = loadE2eFixtures();

  for (const fixture of fixtures) {
    const nameToLog = fixture.irFile || fixture.baseName;

    if (fixture.error) {
      it(`should throw ${fixture.error.name} for ${nameToLog}`, async () => {
        await assert.rejects(
          async () => {
            const { resolvedStream, bodyStream } = hydrate(fixture.template, fixture.data);
            await parse(resolvedStream, bodyStream);
          },
          (err) => err.name === fixture.error.name
        );
      });
      continue;
    }

    it(`should parse ${nameToLog} into the expected IR`, async () => {
      const { resolvedStream, bodyStream } = hydrate(fixture.template, fixture.data);
      const { ir } = await parse(resolvedStream, bodyStream);
      assert.deepEqual(ir, fixture.ir);
    });

    it(`should hydrate ${fixture.baseName} to the expected resolved text and map`, async () => {
      const expectedResolved = fs.readFileSync(path.join(E2E_DIR, `${fixture.baseName}.httpt-r`), 'utf8');
      const expectedMap = JSON.parse(fs.readFileSync(path.join(E2E_DIR, `${fixture.baseName}.httpt-map`), 'utf8'));
      const { head: expHead, body: expBody } = splitBoundary(expectedResolved);

      // A `provided` body keeps an out-of-band placeholder in .httpt-r that the
      // hydrator drops from the execution body stream, so only assert the body
      // for inline (text/json/base64) bodies.
      const isProvided =
        /(^|\n):httpt-body-type:[ \t]*provided/i.test(expectedResolved) ||
        (fixture.data && fixture.data.body && fixture.data.body.type === 'provided');

      const { head, map, body } = await hydrateAll(fixture.template, fixture.data);

      assert.equal(head, expHead, 'resolved head');
      assert.deepEqual(map, expectedMap, 'index-shift map');
      if (!isProvided) {
        assert.equal(body, expBody || '', 'resolved body');
      }
    });
  }
});

describe('Pipeline: line endings', () => {
  it('handles a CRLF (\\r\\n\\r\\n) boundary end-to-end', async () => {
    const template =
      'POST /crlf HTTP/1.1\r\n' +
      'Host: {{ host | raw }}\r\n' +
      'Content-Type: text/plain\r\n' +
      '\r\n' +
      'line-one\r\nline-two\r\n';
    const { resolvedStream, bodyStream } = hydrate(template, { host: 'api.test' });
    const { ir } = await parse(resolvedStream, bodyStream);

    assert.equal(ir.method, 'POST');
    assert.equal(ir.uri, '/crlf');
    assert.equal(ir.host, 'api.test');
    assert.deepEqual(ir.headers, [{ name: 'Content-Type', value: 'text/plain' }]);
    assert.deepEqual(ir.body, { type: 'text', content: 'line-one\r\nline-two' });
  });
});
