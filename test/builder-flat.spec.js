/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const toIterator = require('pull-stream-to-async-iterator')
const pullBuilder = require('../src/builder/flat')
const all = require('async-iterator-all')

const builder = (source, reduce, options) => {
  return toIterator(
    pull(
      values(source),
      pullBuilder(reduce, options)
    )
  )
}

function reduce (leaves, callback) {
  if (leaves.length > 1) {
    callback(null, { children: leaves })
  } else {
    callback(null, leaves[0])
  }
}

describe('builder: flat', () => {
  it('reduces one value into itself', async () => {
    const source = [1]
    const result = await all(builder(source, reduce))

    expect(result).to.be.eql([1])
  })

  it('reduces 2 values into parent', async () => {
    const source = [1, 2]
    const result = await all(builder(source, reduce))

    expect(result).to.be.eql([{ children: [1, 2] }])
  })
})
