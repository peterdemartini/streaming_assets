'use strict'
/* global describe, it, expect */

const processor = require('../asset/transform')
const harness = require('teraslice_op_test_harness')(processor)

const _ = require('lodash')

const H = require('../asset/node_modules/highland')

const inputRecords = [
  { host: 'example.com' },
  { host: 'www.example.com' },
  { host: 'example.co.uk' },
  { host: 'www.example.co.uk' }
]

describe('transform', () => {
  it('should map each input value', () => {
    const opConfig = {
      tx: 'map',
      args: 'x',
      fn: 'x.host += "-test"; return x;'
    }

    const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig)

    results.toArray((values) => {
      expect(values.length).toEqual(4)

      expect(values[0].host).toEqual('example.com-test')
      expect(values[1].host).toEqual('www.example.com-test')
      expect(values[2].host).toEqual('example.co.uk-test')
      expect(values[3].host).toEqual('www.example.co.uk-test')
    })
  })

  it('should find the specified value', () => {
    const opConfig = {
      tx: 'findWhere',
      obj: { host: 'example.com' }
    }

    const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig)

    results.toArray((values) => {
      expect(values.length).toEqual(1)

      expect(values[0].host).toEqual('example.com')
    })
  })

  it('should find the latest value from the stream', () => {
    const opConfig = {
      tx: 'latest'
    }

    const results = harness.run(H(_.cloneDeep(inputRecords)), opConfig)

    results.toArray((values) => {
      expect(values.length).toEqual(1)

      expect(values[0].host).toEqual('www.example.co.uk')
    })
  })
})
