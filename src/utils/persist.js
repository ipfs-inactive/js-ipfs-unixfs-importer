'use strict'

const {
  util
} = require('ipld-dag-pb')
const multicodec = require('multicodec')

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

  if (hashAlg !== 'sha2-256') {
    cidVersion = 1
  }

  if (options.onlyHash) {
    return util.cid(node, {
      version: cidVersion,
      hashAlg: hashAlg
    }, (err, cid) => {
      callback(err, {
        cid,
        node
      })
    })
  }

  // The IPLD expects the format and hashAlg as constants
  if (typeof codec === 'string') {
    const constantName = codec.toUpperCase().replace(/-/g, '_')
    codec = multicodec[constantName]
  }
  if (typeof hashAlg === 'string') {
    const constantName = hashAlg.toUpperCase().replace(/-/g, '_')
    hashAlg = multicodec[constantName]
  }

  ipld.put(node, codec, {
    cidVersion,
    hashAlg
  }).then(
    (cid) => callback(null, {
      cid,
      node
    }),
    (error) => callback(error)
  )
}

module.exports = persist
