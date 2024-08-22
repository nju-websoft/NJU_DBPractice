# WSDB: A Toy RDBMS for NJU "Introduction to Databases" Course

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)    [![Github Star](https://img.shields.io/github/stars/nju-websoft/NJU_DBPractice.svg)](https://github.com/nju-websoft/NJU_DBPractice)

## Introduction

This repository is a toy RDBMS for NJU "Introduction to Databases" course. It is implemented in C++ and supports a
subset of SQL queries.
Six Labs are included in this repository, and the details of each lab are as follows:

* Lab 1: Storage, implement the disk manager and the buffer manager.
* Lab 2: Executor, Implement `INSERT`, `UPDATE`, and `DELETE` queries and basic operators like filter, sort, etc.
* Lab 3. Executor, Implement `JOIN` and `AGGREGATE` queries.
* Lab 4 (future): Index, implement the index manager and B+ tree index.
* Lab 5 (future): Concurrency, implement SS2PL concurrency control and deadlock detection.
* Lab 6 (future): Recovery, implement log manager and recovery manager to support WAL/ARIES recovery algorithm.

The project referenced [BusTub](https://github.com/cmu-db/bustub), [Rucbase](https://github.com/ruc-deke/rucbase-lab)
,and[MiniOB](https://github.com/oceanbase/miniob). Thanks for their great work!

## System requirements

We only tested WSDB on MacOS 14.5 and Ubuntu 20.04, but if it also works on other systems, please let us know by issuing
or pulling requests.

## How to build

First clone the repository from github.

```shell
$ git clone --recursive https://github.com/nju-websoft/NJU_DBPractice
```

Install requirments using package manager.

For Ubuntu or Debian:

```shell
$ sudo apt install gcc g++ cmake flex bison
```

For MacOS:

```shell
$ sudo apt install clang cmake flex bison
```

Then change directory into the repository foot and run

```shell
$ makedir build && cd build
```

```shell
cmake .. && make -j8
```

About how to change cmake configurations, please refer to their guide.

## Tutorial

The Chinese version of the lab tutorial can be found under `docs`, please read them carefully before coding if you are
studying NJU DB course.

## Copyright

```
/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/
```

