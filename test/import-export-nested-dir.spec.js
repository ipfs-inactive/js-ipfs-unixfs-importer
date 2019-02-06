/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const collect = require('pull-stream/sinks/collect')
const map = require('async/map')
const CID = require('cids')

const importer = require('../src')
const exporter = require('ipfs-unixfs-exporter')

describe('import and export: directory', () => {
  const rootHash = 'bafybeig452tftu3cxeeynb76ihksupbmxt3uo2ypbqeyyg7ivy3ky7sqda'
  let ipld

  before((done) => {
    inMemory(IPLD, (err, resolver) => {
      expect(err).to.not.exist()

      ipld = resolver

      done()
    })
  })

  it('imports', function (done) {
    this.timeout(20 * 1000)

    pull(
      values([
        { path: 'a/b/c/d/e', content: values([Buffer.from('banana')]) },
        { path: 'a/b/c/d/f', content: values([Buffer.from('strawberry')]) },
        { path: 'a/b/g', content: values([Buffer.from('ice')]) },
        { path: 'a/b/h', content: values([Buffer.from('cream')]) }
      ]),
      importer(ipld),
      collect((err, files) => {
        expect(err).to.not.exist()
        expect(files.map(normalizeNode).sort(byPath)).to.be.eql([
          { path: 'a/b/h',
            multihash: 'bafkreihtuiwbz2hauwuwhe3qhadojlilmmbr6b4cygznq7orox6enkowhu' },
          { path: 'a/b/g',
            multihash: 'bafkreicmwczfbrrfbvji6ptb2b6zocesc2g3nc576hvfxzfikhw2ienn7m' },
          { path: 'a/b/c/d/f',
            multihash: 'bafkreic6on7yshnrc5kefi473zz6khlydjkfkbwxdskuo6tn5nmyrpl7ti' },
          { path: 'a/b/c/d/e',
            multihash: 'bafkreifuspkigzfp4rgrdqawlt2hbjawjupcmcmrd34zrpugrvdk3y66jy' },
          { path: 'a/b/c/d',
            multihash: 'bafybeih327xgvrvrskzp2szg2h4mj2kppxubbb5go5dil2gdsqkdhctlga' },
          { path: 'a/b/c',
            multihash: 'bafybeihepdheh6iqyrmwlcsuy4wbxsbbjqvqycs6eps5wqnsmdboqq5ow4' },
          { path: 'a/b',
            multihash: 'bafybeieolgnh4uy6vp5wc5qij6ymhk3coh2mto4q57wmzzab3jfqfp66rq' },
          { path: 'a',
            multihash: rootHash }
        ])
        done()
      })
    )
  })

  it('exports', function (done) {
    this.timeout(20 * 1000)

    pull(
      exporter(rootHash, ipld),
      collect((err, files) => {
        expect(err).to.not.exist()
        map(
          files,
          (file, callback) => {
            if (file.content) {
              pull(
                file.content,
                collect(mapFile(file, callback))
              )
            } else {
              callback(null, { path: file.path })
            }
          },
          (err, files) => {
            expect(err).to.not.exist()
            expect(files.filter(fileHasContent).sort(byPath)).to.eql([
              { path: 'bafybeig452tftu3cxeeynb76ihksupbmxt3uo2ypbqeyyg7ivy3ky7sqda/b/h',
                content: 'cream' },
              { path: 'bafybeig452tftu3cxeeynb76ihksupbmxt3uo2ypbqeyyg7ivy3ky7sqda/b/g',
                content: 'ice' },
              { path: 'bafybeig452tftu3cxeeynb76ihksupbmxt3uo2ypbqeyyg7ivy3ky7sqda/b/c/d/f',
                content: 'strawberry' },
              { path: 'bafybeig452tftu3cxeeynb76ihksupbmxt3uo2ypbqeyyg7ivy3ky7sqda/b/c/d/e',
                content: 'banana' }
            ])
            done()
          })
      })
    )

    function mapFile (file, callback) {
      return (err, fileContent) => {
        callback(err, fileContent && {
          path: file.path,
          content: fileContent.toString()
        })
      }
    }
  })
})

function normalizeNode (node) {
  return {
    path: node.path,
    multihash: new CID(node.multihash).toBaseEncodedString()
  }
}

function fileHasContent (file) {
  return Boolean(file.content)
}

function byPath (a, b) {
  if (a.path > b.path) return -1
  if (a.path < b.path) return 1
  return 0
}
