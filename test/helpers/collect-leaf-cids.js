'use strict'

module.exports = function (cid, ipld) {
  async function * traverse (cid) {
    const node = await ipld.get(cid)

    if (Buffer.isBuffer(node) || !node.links.length) {
      yield {
        node,
        cid
      }

      return
    }

    node.links.forEach(link => traverse(link.cid))
  }

  return traverse(cid)
}
