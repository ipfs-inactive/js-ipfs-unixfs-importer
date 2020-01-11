'use strict'

const UnixFS = require('ipfs-unixfs')
const persist = require('../utils/persist')

const dagNodeBuilder = async (path, node, ipld, options) => {
  const cid = await persist(node, ipld, options)

  return {
    cid,
    path,
    unixfs: UnixFS.unmarshal(node.Data),
    node
  }
}

module.exports = dagNodeBuilder
