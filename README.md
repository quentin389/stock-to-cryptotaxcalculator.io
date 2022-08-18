# Stock To cryptotaxcalculator.io (UK only)

## Purpose

UK taxes are, in the simplest situation, the same for crypto and stock.

This set of scripts parses stock transaction files from several example
stock exchanges formats to cryptotaxcalculator.io "Advanced Manual CSV" format,
by making the stock appear as custom coins and faking some transaction types
in order to trick the calculator into doing your stock taxes along with the crypto
taxes. Magic!

This is done solely for my own needs, with file formats I had available at the time
of the making, works only in the UK, and you should never do anything with your
taxes without consulting your accountant or making SURE you know the tax code.

## Available Conversions

This parser accepts the following formats:
* eToro xls format: `etoro-account-statement-*.xlsx` files

and that's it
