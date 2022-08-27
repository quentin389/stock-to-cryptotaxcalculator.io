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

## Correct file formats

Please note that in many places I throw errors if the files being parsed are not
**exactly** in the same format as the files I had as examples. That is because
I can't predict or assume that some variation means something specific without
analysing it. So it's better to say "stop, I don't know what is this" than allow
a deviation and produce random output.

This means that it's possible that almost any new file being parsed will throw
an error.

Also, keep in mind that cryptotaxcalculator.io format and rules also change,
so even with source files in correct format, in time something may change and
stop producing desired effects.

## Available Conversions

This parser accepts the following formats:
* eToro xls format: `etoro-account-statement-*.xlsx` files. This source file has to
  be generated using the "xls" button on eToro "Account statement" page.
* IBKR (Interactive Brokers) csv format: ...

and that's it

## How to use

