'use strict'

const mh = require('multihashes')
const mc = require('multicodec')

const {
  util: {
    cid
  }
} = require('ipld-dag-pb')

const defaultOptions = {
  cidVersion: 0,
  hashAlg: 'sha2-256',
  codec: 'dag-pb'
}

const persist = (node, ipld, options, callback) => {
  let cidVersion = options.cidVersion || defaultOptions.cidVersion
  let hashAlg = options.hashAlg || defaultOptions.hashAlg
  let codec = options.codec || defaultOptions.codec

  if (Buffer.isBuffer(node)) {
    cidVersion = 1
    codec = 'raw'
  }

  if (hashAlg !== 'sha2-256' && hashAlg !== mh.names['sha2-256']) {
    cidVersion = 1
  }

  if (isNaN(hashAlg)) {
    hashAlg = mh.names[hashAlg]
  }

  if (options.onlyHash) {
    return cid(node, {
      version: cidVersion,
      hashAlg: hashAlg
    }, (err, cid) => {
      callback(err, {
        cid,
        node
      })
    })
  }

  ipld.put(node, mc[codec.toUpperCase().replace(/-/g, '_')], {
    cidVersion: cidVersion,
    hashAlg: hashAlg
  })
    .then((cid) => callback(null, {
      cid,
      node
    }), callback)
}

module.exports = persist
