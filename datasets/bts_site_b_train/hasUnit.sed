#! /usr/bin/env sed -E

s/brick:hasUnit \[ brick:value "([^"]+)" ]/brick:hasUnit unit:\1/
