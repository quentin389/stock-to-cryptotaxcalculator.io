# Stock To cryptotaxcalculator.io (UK only)

## Warnings
* I do not guarantee that anything here will work or will be HMRC-compliant.
* I have created this based on my transactions files.
  Many things are not implemented.
* Do not upload multiple manual files covering the same transactions or re-upload
  the same file in cryptotaxcalculator.io. This may mess things up.
  Delete the old file first, then add the new one.

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
* eToro xls format: `etoro-account-statement-*.xlsx` files. This source file has to
  be generated using the "xls" button on eToro "Account statement" page.

and that's it

## How to use

