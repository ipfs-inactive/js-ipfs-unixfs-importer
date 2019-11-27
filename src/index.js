'use strict'

const dagBuilder = require('./dag-builder')
const treeBuilder = require('./tree-builder')
const parallelBatch = require('it-parallel-batch')
const mergeOptions = require('merge-options').bind({ ignoreUndefined: true })

const defaultOptions = {
  chunker: 'fixed',
  strategy: 'balanced', // 'flat', 'trickle'
  rawLeaves: false,
  onlyHash: false,
  reduceSingleLeafToSelf: true,
  codec: 'dag-pb',
  hashAlg: 'sha2-256',
  leafType: 'file', // 'raw'
  cidVersion: 0,
  progress: () => () => {},
  shardSplitThreshold: 1000,
  fileImportConcurrency: 50,
  blockWriteConcurrency: 10,
  minChunkSize: 262144,
  maxChunkSize: 262144,
  avgChunkSize: 262144,
  window: 16,
  polynomial: 17437180132763653, // https://github.com/ipfs/go-ipfs-chunker/blob/d0125832512163708c0804a3cda060e21acddae4/rabin.go#L11
  maxChildrenPerNode: 174,
  layerRepeat: 4,
  wrapWithDirectory: false,
  pin: true,
  recursive: false,
  ignore: null, // []
  hidden: false,
  preload: true
}

module.exports = async function * (source, ipld, options = {}) {
  const opts = mergeOptions(defaultOptions, options)

  if (options.cidVersion > 0 && options.rawLeaves === undefined) {
    // if the cid version is 1 or above, use raw leaves as this is
    // what go does.
    opts.rawLeaves = true
  }

  if (options.hashAlg !== undefined && options.rawLeaves === undefined) {
    // if a non-default hash alg has been specified, use raw leaves as this is
    // what go does.
    opts.rawLeaves = true
  }

  // go-ifps trickle dag defaults to unixfs raw leaves, balanced dag defaults to file leaves
  if (options.strategy === 'trickle') {
    opts.leafType = 'raw'
    opts.reduceSingleLeafToSelf = false
  }

  if (options.format) {
    opts.codec = options.format
  }

  for await (const entry of treeBuilder(parallelBatch(dagBuilder(source, ipld, opts), opts.fileImportConcurrency), ipld, opts)) {
    yield {
      cid: entry.cid,
      path: entry.path,
      unixfs: entry.unixfs,
      size: entry.size
    }
  }
}
