# Audience Manager API : Python wrapper

This project is a python wrapper that enable to use the Audience Manager API, with the adobe.io integration.\
The wrapper follows the same architecture than the [aanalytics2 wrapper](https://github.com/pitchmuc/adobe_analytics_api_2.0).\
Following this architecture, the GET methods are regrouped together and the POST methods are used with the "create" prefix.\
The complete documentation of the Audience Manager manager can be found here : [Audience Manager API swagger](https://bank.demdex.com/portal/swagger/index.html#)

## Installation

You can install the module by using the pip method as follows:

```shell
pip install audiencemanager
```

in order to upgrade the module when a new release is published, you can use the following command:

```shell
pip install --upgrade audiencemanager
```

## Getting Started

There is a documentation to gets you started on the audience manager API.\
Please refer to [the documentation](./docs/get-started.md) in case you want to get more example.

As general guidelines:

* create a adobe.io connection.
* set the adobe.io connection with admin right in AAM tool.
* import the audiencemanager module.
* create and / or import the config file that you can generate from the library.
* instantiate an AAM instance after importing the config file.
* use the docstrings in order to know more about the methods.

## Releases

If you want to know more about the different releases of this library, you can refer to the [releases page](./docs/releases.md).
