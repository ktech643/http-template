const { describe, it } = require('node:test');
const assert = require('node:assert');
const { hydrate, parse } = require('../src/pipeline.js');
const { validateStreamReferences } = require('../src/stream-refs.js');

async function resolvedHead(template, data, streams) {
  const { resolvedStream, bodyStream } = hydrate(template, data, streams);
  let head = '';
  for await (const chunk of resolvedStream) head += chunk;
  await bodyStream.getReader().read().catch(() => {});
  return head;
}

async function hydrateParse(template, data, streams) {
  const { resolvedStream, bodyStream } = hydrate(template, data, streams);
  return parse(resolvedStream, bodyStream);
}

describe('§1.4.4 stream reference validation (unit)', () => {
  it('accepts a single reference, explicit or implicit', () => {
    assert.doesNotThrow(() => validateStreamReferences([{ index: 0, explicit: false, where: 'body' }]));
    assert.doesNotThrow(() => validateStreamReferences([{ index: 3, explicit: true, where: 'body' }]));
  });

  it('accepts multiple references with distinct explicit indices', () => {
    assert.doesNotThrow(() =>
      validateStreamReferences([
        { index: 0, explicit: true, where: 'filter:a' },
        { index: 1, explicit: true, where: 'filter:b' },
      ])
    );
  });

  it('throws AmbiguousStreamReferenceError when >1 ref and any is implicit', () => {
    assert.throws(
      () =>
        validateStreamReferences([
          { index: 0, explicit: false, where: 'body' },
          { index: 1, explicit: true, where: 'filter:a' },
        ]),
      (err) => err.name === 'AmbiguousStreamReferenceError'
    );
  });

  it('throws DuplicateStreamReferenceError on repeated indices', () => {
    assert.throws(
      () =>
        validateStreamReferences([
          { index: 0, explicit: true, where: 'filter:a' },
          { index: 0, explicit: true, where: 'filter:b' },
        ]),
      (err) => err.name === 'DuplicateStreamReferenceError'
    );
  });
});

describe('streaming filters (§1.3.2.3) + validation end-to-end', () => {
  it('stream-as-base64 injects the base64 of a materialized stream', async () => {
    const head = await resolvedHead(
      'POST /u HTTP/1.1\nHost: api.test\nX-Data: {{ img | stream-as-base64 }}\n',
      { img: { type: 'provided', content: 0 } },
      [Buffer.from('hello')]
    );
    assert.match(head, /X-Data: aGVsbG8=/); // base64('hello')
  });

  it('stream-as-utf8 injects the decoded stream text', async () => {
    const head = await resolvedHead(
      'POST /u HTTP/1.1\nHost: api.test\nX-Text: {{ t | stream-as-utf8 }}\n',
      { t: { type: 'provided', content: 0 } },
      ['world']
    );
    assert.match(head, /X-Text: world/);
  });

  it('resolves two distinct explicit references', async () => {
    const head = await resolvedHead(
      'GET / HTTP/1.1\nHost: x\nA: {{ s1 | stream-as-utf8 }}\nB: {{ s2 | stream-as-utf8 }}\n',
      { s1: { content: 0 }, s2: { content: 1 } },
      ['AA', 'BB']
    );
    assert.match(head, /A: AA/);
    assert.match(head, /B: BB/);
  });

  it('rejects with AmbiguousStreamReferenceError when a second reference is implicit', async () => {
    await assert.rejects(
      () =>
        hydrateParse(
          'GET / HTTP/1.1\nHost: x\nA: {{ s1 | stream-as-utf8 }}\nB: {{ s2 | stream-as-utf8 }}\n',
          { s1: { type: 'provided' }, s2: { type: 'provided', content: 1 } },
          ['AA', 'BB']
        ),
      (err) => err.name === 'AmbiguousStreamReferenceError'
    );
  });

  it('rejects with DuplicateStreamReferenceError on repeated indices', async () => {
    await assert.rejects(
      () =>
        hydrateParse(
          'GET / HTTP/1.1\nHost: x\nA: {{ s1 | stream-as-utf8 }}\nB: {{ s2 | stream-as-utf8 }}\n',
          { s1: { content: 0 }, s2: { content: 0 } },
          ['AA', 'BB']
        ),
      (err) => err.name === 'DuplicateStreamReferenceError'
    );
  });

  it('applies the implicit default (index 0) for a lone provided body', async () => {
    const { ir } = await hydrateParse(
      'PUT /u HTTP/1.1\nHost: api.test\n:httpt-body-type: provided\n',
      {},
      ['payload']
    );
    assert.deepEqual(ir.body, { type: 'provided', content: 0 });
  });
});
