# NJUDB: A Toy RDBMS for NJU "Introduction to Databases" Course

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)    [![Github Star](https://img.shields.io/github/stars/nju-websoft/NJU_DBPractice.svg)](https://github.com/nju-websoft/NJU_DBPractice)

## Introduction

This repository is a toy RDBMS for NJU "Introduction to Databases" course. It is implemented in C++ and supports a
subset of SQL queries.

Four Labs are included in this repository, and the details of each lab are as follows:

* **Lab01**: Buffer Pool - Implement the disk manager and buffer pool manager
* **Lab02**: Executor Basic - Implement `INSERT`, `UPDATE`, `DELETE` and basic operators (filter, sort, etc.)
* **Lab03**: Executor Analysis - Implement `JOIN` and `AGGREGATE` queries  
* **Lab04**: Index & Storage Index - Implement B+ tree, hash index, and index scan executor
* **Lab05** (future): Concurrency, implement SS2PL concurrency control and deadlock detection.
* **Lab06** (future): Recovery, implement log manager and recovery manager to support WAL/ARIES recovery algorithm.

The project is mostly inspired by [Rucbase](https://github.com/ruc-deke/rucbase-lab)
, and some components follow the design of
 [BusTub](https://github.com/cmu-db/bustub),  [MiniOB](https://github.com/oceanbase/miniob). Thanks for their great work!


## Lab Configuration System

NJUDB features a flexible configuration system that allows you to choose between compiling labs from source or using pre-compiled gold-standard libraries for each lab individually.

### Quick Start

```bash
# Safe default - compile everything from source
./configure.sh --all-source --clean

# Working on Lab01 - use gold dependencies
./configure.sh --lab02-gold --clean

# Working on Lab02 - use Lab01 as gold dependency  
./configure.sh --lab01-gold --clean

# Get help and see all options, we suggest using --clean for all configurations to avoid cmake warnings.
./configure.sh --help
```

The project referenced [BusTub](https://github.com/cmu-db/bustub), [Rucbase](https://github.com/ruc-deke/rucbase-lab)
,and[MiniOB](https://github.com/oceanbase/miniob). Thanks for their great work!
Other document references: [TiDB](https://docs.pingcap.com/), [Oracle](https://docs.oracle.com/search/?q=Oracle%20Data%20Types&pg=1&size=10&library=en%2Fdatabase%2Foracle%2Foracle-database%2F23&book=SQLQR&lang=en) and [PostgreSQL](https://www.postgresql.org/docs/current/sql-commands.html).

## System requirements

We only tested NJUDB on MacOS and Ubuntu, but if it also works on other systems, please let us know by issuing
or pulling requests.

## How to build

First clone the repository from github.

```shell
$ git clone --recursive https://github.com/nju-websoft/NJU_DBPractice
```

Install requirments using package manager.

For Ubuntu or Debian:

```shell
$ sudo apt install gcc g++ cmake flex bison libreadline-dev
```

For MacOS:

```shell
$ sudo brew install clang cmake flex bison readline
```

Then change directory into the repository foot and run

```shell
$ mkdir build && cd build
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

