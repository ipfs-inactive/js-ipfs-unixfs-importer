/* eslint-disable no-console */
/* eslint-env mocha */
'use strict'

const importer = require('../src')
const IPLD = require('ipld')
const bufferStream = require('async-iterator-buffer-stream')
const all = require('async-iterator-all')
const randomBuffer = require('iso-random-stream/src/random')
const IPFSRepo = require('ipfs-repo')
const BlockService = require('ipfs-block-service')
const tempy = require('tempy')
const Benchmark = require('benchmark')

const createIPLD = async (opts) => {
  const repo = new IPFSRepo(tempy.directory(), opts)
  const blockService = new BlockService(repo)
  await repo.init({})
  await repo.open()
  return new IPLD({ blockService })
}

const FILE_SIZE = Math.pow(2, 20) * 500 // 500MB
const CHUNK_SIZE = 65536

describe('benchmark', function () {
  this.timeout(0)
  it('single run 500mb without batch', async () => { // eslint-disable-line no-loop-func
    const ipld = await createIPLD()
    const times = []
    let read = 0
    let lastDate = Date.now()
    let lastPercent = 0

    const options = {
      batchInterval: 50,
      batch: false,
      progress: (prog) => {
        read += prog
        const percent = parseInt((read / FILE_SIZE) * 100)
        if (percent > lastPercent) {
          times[percent] = (times[percent] || 0) + (Date.now() - lastDate)
          lastDate = Date.now()
          lastPercent = percent
        }
      }
    }

    await all(importer([{
      path: 'single-500.txt',
      content: bufferStream(FILE_SIZE, {
        chunkSize: CHUNK_SIZE,
        generator: () => {
          return randomBuffer(CHUNK_SIZE)
        }
      })
    }], ipld, options))
    // console.info('Percent\tms') // eslint-disable-line no-console
    // times.forEach((time, index) => {
    //   console.info(`${index}\t${parseInt(time)}`) // eslint-disable-line no-console
    // })
  })

  it('single run 500mb with batch 50ms', async () => { // eslint-disable-line no-loop-func
    const ipld = await createIPLD()

    const options = {
      batchInterval: 50,
      batch: true
    }

    await all(importer([{
      path: 'single-500-batch-50.txt',
      content: bufferStream(FILE_SIZE, {
        chunkSize: CHUNK_SIZE,
        generator: () => {
          return randomBuffer(CHUNK_SIZE)
        }
      })
    }], ipld, options))
  })

  it('single run 500mb with batch 100ms', async () => { // eslint-disable-line no-loop-func
    const ipld = await createIPLD()

    const options = {
      batchInterval: 100,
      batch: true
    }

    await all(importer([{
      path: 'single-500-batch.txt',
      content: bufferStream(FILE_SIZE, {
        chunkSize: CHUNK_SIZE,
        generator: () => {
          return randomBuffer(CHUNK_SIZE)
        }
      })
    }], ipld, options))
  })

  it('single run 500mb with batch 150ms', async () => { // eslint-disable-line no-loop-func
    const ipld = await createIPLD()

    const options = {
      batchInterval: 150,
      batch: true
    }

    await all(importer([{
      path: 'single-500-batch-150.txt',
      content: bufferStream(FILE_SIZE, {
        chunkSize: CHUNK_SIZE,
        generator: () => {
          return randomBuffer(CHUNK_SIZE)
        }
      })
    }], ipld, options))
  })
  it('single run 500mb with batch 200ms', async () => { // eslint-disable-line no-loop-func
    const ipld = await createIPLD()

    const options = {
      batchInterval: 200,
      batch: true
    }

    await all(importer([{
      path: 'single-500-batch-200.txt',
      content: bufferStream(FILE_SIZE, {
        chunkSize: CHUNK_SIZE,
        generator: () => {
          return randomBuffer(CHUNK_SIZE)
        }
      })
    }], ipld, options))
  })

  const sizes = [10, 50, 100, 200]

  for (const size of sizes) {
    it(`benchmark ${size}mb`, (done) => {
      const suite = new Benchmark.Suite()
      const FILE_SIZE = Math.pow(2, 20) * size

      suite
        .add('without batch', {
          defer: true,
          fn: async (deferred) => {
            const ipld = await createIPLD()
            await all(importer(
              [{
                path: '200Bytes.txt',
                content: bufferStream(FILE_SIZE, {
                  chunkSize: CHUNK_SIZE,
                  generator: () => randomBuffer(CHUNK_SIZE)
                })
              }],
              ipld,
              { batch: false }
            ))

            deferred.resolve()
          }
        })
        .add('batch 50ms ', {
          defer: true,
          fn: async (deferred) => {
            const ipld = await createIPLD()
            await all(importer(
              [{
                path: 'batch100.txt',
                content: bufferStream(FILE_SIZE, {
                  chunkSize: CHUNK_SIZE,
                  generator: () => randomBuffer(CHUNK_SIZE)
                })
              }],
              ipld,
              { batch: true, batchInterval: 50 }
            ))

            deferred.resolve()
          }
        })
        .add('batch 100ms ', {
          defer: true,
          fn: async (deferred) => {
            const ipld = await createIPLD()
            await all(importer(
              [{
                path: 'batch100.txt',
                content: bufferStream(FILE_SIZE, {
                  chunkSize: CHUNK_SIZE,
                  generator: () => randomBuffer(CHUNK_SIZE)
                })
              }],
              ipld,
              { batch: true, batchInterval: 100 }
            ))

            deferred.resolve()
          }
        })
        .add('batch 150ms ', {
          defer: true,
          fn: async (deferred) => {
            const ipld = await createIPLD()
            await all(importer(
              [{
                path: 'batch100.txt',
                content: bufferStream(FILE_SIZE, {
                  chunkSize: CHUNK_SIZE,
                  generator: () => randomBuffer(CHUNK_SIZE)
                })
              }],
              ipld,
              { batch: true, batchInterval: 150 }
            ))

            deferred.resolve()
          }
        })
        .add('batch 200mb ', {
          defer: true,
          fn: async (deferred) => {
            const ipld = await createIPLD()
            await all(importer(
              [{
                path: 'batch100.txt',
                content: bufferStream(FILE_SIZE, {
                  chunkSize: CHUNK_SIZE,
                  generator: () => randomBuffer(CHUNK_SIZE)
                })
              }],
              ipld,
              { batch: true, batchInterval: 200 }
            ))

            deferred.resolve()
          }
        })
        .on('cycle', function (event) {
          console.log(String(event.target))
        })
        .on('complete', function () {
          console.log('Fastest is ' + this.filter('fastest').map('name'))
          done()
        })
        .run({ async: true })
    })
  }
})
