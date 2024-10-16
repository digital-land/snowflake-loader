## Snowflake Loader

A small repo that can load data from our platform using the pre-baked dataset files and create individual dataframes for each dataset in snowflake. 

### Usage

Before beginning it's advised to create a virtual environment. then run

```
make init
```
To install the required python modules. Environment variables are needed to know the snowflake account to connect to. either set up a .env file or feed them into the make command.

Then run 

```
make load
```

To load the datasets into snowflake. If environment variables aren't present then an error should be raised.