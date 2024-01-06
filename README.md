# pvm, parallel virtual machine

## Introduction

An implementation of [block-stm](https://arxiv.org/pdf/2203.06871.pdf) with code reuse in mind.

## Usage

Follow the example [here](https://github.com/zhiqiangxu/pvm-rs/blob/3ca373a2df69cb6ee0928253cb76bade0beb372d/src/main.rs), basically users only need to provide an implementation for [VM](https://github.com/zhiqiangxu/pvm-rs/blob/3ca373a2df69cb6ee0928253cb76bade0beb372d/src/types.rs#L65) trait.