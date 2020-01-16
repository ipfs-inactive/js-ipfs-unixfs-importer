'use strict'

const mh = require('multihashing-async').multihash
const mc = require('multicodec')

const persist = (node, ipld, options) => {
  if (!options.codec && node.length) {
    options.cidVersion = 1
    options.codec = 'raw'
  }

  if (!options.codec) {
    options.codec = 'dag-pb'
  }

  if (isNaN(options.hashAlg)) {
    options.hashAlg = mh.names[options.hashAlg]
  }

  if (options.hashAlg !== mh.names['sha2-256']) {
    options.cidVersion = 1
  }

  if (options.format) {
    options.codec = options.format
  }

  const format = mc[options.codec.toUpperCase().replace(/-/g, '_')]

  return ipld.put(node, format, options)
}

module.exports = persist
