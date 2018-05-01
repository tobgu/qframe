/*
config acts as a base package for different configuration options used when creating or working with QFrames.

Most of the configs use "functional options" as presented here:
https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

While it is a nice way to overcome the lack of keyword arguments in Go to be able to extend function
signatures in a backwards compatible way the problem with this method (as I see it) is that the
discoverability of existing config options is lacking.

This structure hopes to help (a bit) in fixing that by having a separate package for each configuration
type. That way the function listing of each package conveys all options available for that particular
config option.
*/

package config
