###################################################################################
## Greg Cook
## Geography 485
## Final Project
## Parsing addresses to USPS standards.
## This project is designed to take input from a user via a Python add-in application extension within an
## ArcGIS install. The script is setup to take in a variety of different potential inputs, including a
## geodatabase, XLS or CSV files. Once the user identifies a specified address table, the script performs
## a variety of operations to format the address to USPS standards.

###################################################################################
## Library Import
###################################################################################
import sys,os,re,string
import arcpy,csv,xlrd,pyodbc
from datetime import datetime
from address import AddressParser, Address
arcpy.env.overwriteOutput = True

###################################################################################
## Script Controls
## This should be specified by a user via the ArcGIS script input.
###################################################################################
## Inputs which are required.
inDatabase = None                                                                       ## Path to Geodatabase table, database or file.
selType = None                                                                          ## Can be CSV, XLS, XLSX or Geodatabase. https://code.google.com/p/pyodbc/
inAddressLocator = None                                                                 ## Path to address locator.
inTable = None                                                                          ## Name of table for Geodatabase or database.
inAddressColumn = None                                                                  ## String name of the column containing street addresses in the specified table.
inZipCodeColumn = None                                                                  ## String name of the column containing zipcodes in the specified table.

## Outputs which are required.
outGeodatabase = None

## Outputs which are optional.
outCorrections = None                                                                   ## Output corrections to a table.
outParsedRows = None                                                                    ## Parse addresses to component parts.
outGeocode = None                                                                       ## Geocode the addresses.
selJoinDatasetType = None                                                               ## Spatial Join to 2010 Census Block or 2010 Census Block Group.

#####################################################################################
## Example script settings
#####################################################################################
inDatabase = r"C:\GIS\485\Final Project\TestAddress.xls"
selType = "XLS"
inAddressLocator = r"C:\GIS\485\Final Project\BackgroundData.gdb\Countywide_911_Address_Locator"
inTable = "TestAddress"
inAddressColumn = "ADDRESS"
inZipCodeColumn = "ZIPCODE"
outGeodatabase = r"C:\GIS\485\Final Project\OutputData.gdb"
outCorrections = True
outParsedRows = True
outGeocode = True
selJoinDatasetType = "2010 Census Block"
arcpy.env.workspace = outGeodatabase

###################################################################################
## Script controls
###################################################################################
approvedJoinList = ["2010 Census Block","2010 Census Block Group"]
approvedParseList = ["HOUSE_NBR","STREET_PRE","STREET_NAME","STREET_SUF","STREET_APT"]
columnList = ["UNIQUE_ROW","ADDRESS","ZIP_CODE"]
errorRowList = []                                                                       ## Holds row numbers which have an error detected in them.
formatAddressList = []
formatZipList = []                                                                      ## Name of files automatically created under the Geodatabase.
outTable = "Address_List"
outCorrectionTable = "Address_Errors"
outGeocode = "Geocoded_Addresses"
outJoinDataset = "Joined_Addresses"
tablePath = "{0}\{1}".format(outGeodatabase, outTable)
correctionTablePath = "{0}\{1}".format(outGeodatabase, outCorrectionTable)

def main():
####################################################################################
## Calls each part of the script.
####################################################################################
userInput()
arcpy.AddMessage(selType)
checkFileType(inDatabase, selType, inTable, inAddressColumn, inZipCodeColumn)

if outGeocode is True:
    geocodeAddress(inTable, inAddressLocator, outGeocode)

if outGeocode is True and outJoinDatasetLocation is not None and selJoinDatasetType is not None:
    spatialJoin(outGeocode, outJoinDatasetLocation)

def userInput():
####################################################################################
## Obtains user input from arcpy
####################################################################################
inDatabase = arcpy.GetParameterAsText(0)                                                ## Inputs which are required.
selType = arcpy.GetParameterAsText(1)
inAddressLocator = arcpy.GetParameterAsText(2)
inTable = arcpy.GetParameterAsText(3)
inAddressColumn = arcpy.GetParameterAsText(4)
inZipCodeColumn = arcpy.GetParameterAsText(5)
outGeodatabase = arcpy.GetParameterAsText(6)                                            ## Outputs which are required.
outCorrections = arcpy.GetParameterAsText(7)                                            ## Outputs which are optional. Shown as textboxes and entered as booleans under the script properties.
outParsedRows = arcpy.GetParameterAsText(8)
outGeocode = arcpy.GetParameterAsText(9)
outJoinDatasetType = arcpy.GetParameterAsText(10)                                       ## Output which is optional and entered as a string.
arcpy.env.workspace = outGeodatabase                                                    ## Set the workspace to the output geodatabase.

def checkFileType(fileLocation, inType, inTable, inAddressColumn, inZipCodeColumn):
####################################################################################
## Check the file type specified by user input.
####################################################################################
filePath = "{0}\{1}".format(fileLocation, inTable)                                      ## Determine the file path.
fields = [inAddressColumn, inZipCodeColumn]
if selType == "Geodatabase":
parseGeodatabase(filePath, fields)
arcpy.env.workspace = fileLocation
elif selType == "CSV":
parseCSV(fileLocation)
elif selType == "XLS":
parseXLS(fileLocation)
else:
arcpy.AddMessage(selType)
## arcpy.AddMessage("File type not supported. Input geodatabase, .csv, .xls or .xlsx file.")

def parseGeodatabase(inputFile, fieldsToParse):
####################################################################################
## Parse a geodatabase input.
####################################################################################
arcpy.AddMessage("Parsing Geodatabase.")
addressPull = []
zipPull = []
rowCounter = 0
with arcpy.da.SearchCursor(inputFile, fieldsToParse) as cursor:                         ## Pull addresses and zip codes from a feature class.
for row in cursor:
    addressPull.append(row[0])
    zipPull.append(row[1])
    generateTable(addressPull,zipPull)

def parseCSV(inputFile):                                                                ## Use the reader method to read the csv file.
####################################################################################
## Parse a csv input.
####################################################################################
arcpy.AddMessage("Parsing CSV.")
addressPull = []
zipPull = []

with open(inputFile, 'rb') as infile:
    reader = csv.reader(open(inputFile, 'rb'), delimiter=',', quotechar='"')            ## Based on the default values, the first row in the csv is assumed to be the header.
    next(reader, None)                                                                  ## Skip the header of the csv file.
for row in reader:                                                                      ## Increment through each row in the csv file.
    addressPull.append(''.join(row[1]))
    zipPull.append(''.join(row[2]))

generateTable(addressPull,zipPull)

def parseXLS(inputFile):
####################################################################################
## Parse an xls file.
####################################################################################
arcpy.AddMessage("Parsing XLS.")
addressPull = []
zipPull = []

with xlrd.open_workbook(inputFile) as wb:
    worksheet = wb.sheet_by_index(0)                                                    ## Open the first worksheet in the file.
    numRows = worksheet.nrows - 1                                                       ## Determine the total number of rows in the worksheet.
    curRow = 0                                                                          ## Set the value of the current row to 0 befor running the loop.
while curRow < numRows:                                                                 ## Loops thorugh the worksheet by row.
    curRow += 1                                                                         ## Increments the current row by 1.
    addressPull.append(worksheet.cell_value(curRow,1))
    zipPull.append(str(worksheet.cell_value(curRow,2)))

generateTable(addressPull,zipPull)

def generateTable(addressList, zipList):
    arcpy.AddMessage("Creating address table.")
addressHolder = addressFormat(addressList)
zipHolder = zipFormat(zipList)
arcpy.CreateTable_management(outGeodatabase, outTable)                                  ## Create address output table.
addFields(tablePath)                                                                    ## Add fields to address output table.
rowCounter = 0

if outParsedRows == True:                                                               ## Output parsed rows if checked by user.
arcpy.AddMessage("Parsing addresses.")
with arcpy.da.InsertCursor(tablePath, columnList + approvedParseList) as cursor:
    for entry in addressHolder:
        addressInsert = ''.join(addressHolder[rowCounter])
        zipInsert = ''.join(zipHolder[rowCounter])
        
if rowCounter in errorRowList:                                                          ## Skips rows where an error has been found.
pass
else :
    houseNumber, streetPrefix, street, streetSuffix, apartment = addressParse(
        addressInsert)
cursor.insertRow((rowCounter, addressInsert, zipInsert, houseNumber,
    streetPrefix, street, streetSuffix, apartment))
rowCounter += 1
else :
    with arcpy.da.InsertCursor(tablePath, columnList) as cursor:
    for entry in addressHolder:
    addressInsert = ''.join(addressHolder[rowCounter])
zipInsert = ''.join(zipHolder[rowCounter])
if rowCounter in errorRowList:                                                          ## Skips rows where an error has been found.
pass
else :
    cursor.insertRow((rowCounter, addressInsert, zipInsert))                            ## Insert values into the table with the InsertCursor.
rowCounter += 1
rowCounter = 0                                                                          ## Reset the rowCounter variable.
if outCorrections == True:                                                              ## Output correction table
if checked by user.
arcpy.AddMessage("Creating correction table.")
arcpy.CreateTable_management(outGeodatabase, outCorrectionTable)                        ## Create corrections table.
addFields(correctionTablePath)                                                          ## Add fields to corrections table.
with arcpy.da.InsertCursor(correctionTablePath, columnList) as cursor:
    for entry in addressHolder:
    if rowCounter in errorRowList:
    addressInsert = ''.join(addressHolder[rowCounter])
zipInsert = ''.join(zipHolder[rowCounter])
cursor.insertRow((rowCounter, addressInsert, zipInsert))
else :
    pass
rowCounter += 1

def addressFormat(inputAddresses):
####################################################################################
## Formats input addresses to match specific address standards.
####################################################################################
arcpy.AddMessage("Formatting Addresses.")
rowCounter = 0
for row in inputAddresses:
    formatAddress = ''.join(inputAddresses[rowCounter])
    formatAddress = formatAddress.upper()                                               ## Change string to uppercase.
    formatAddress = formatAddress.strip()                                               ## Remove any whitespace on each end of the string.
if not formatAddress:                                                                   ## Initial Error checking to remove problematic addresses to a correction table.
    errorRowList.append(rowCounter)                                                     ## Check if string is blank.
if formatAddress[: 3].isdigit() == False:                                               ## Check if string isn 't a digit for the first 3 characters.
if formatAddress[: 1].isdigit() == False and formatAddress[1: 3].isdigit() == True:     ## Rough check for fire numbers.
errorRowList.append(rowCounter)
if formatAddress.find('#') != -1:
    formatAddress = formatAddress[: formatAddress.find('#') - 1]                        ## Truncate string to a# sign for apartment numbers.
if formatAddress.find('/') != -1:
    formatAddress = formatAddress[: formatAddress.find('/')]                            ## Remove one character on both sides of a / for partial street numbers.
if formatAddress.find('REAR') != -1:
    formatAddress = formatAddress.replace('REAR', '')                                   ## The following is a series of checks
for specific strings relating to apartment types.
if formatAddress.find('UPPER') != -1:
    formatAddress = formatAddress.replace('UPPER', '')
if formatAddress.find('UPPR') != -1:
    formatAddress = formatAddress.replace('UPPR', '')
if formatAddress.find('LOWER') != -1:
    formatAddress = formatAddress.replace('LOWER', '')
if formatAddress.find('LOWR') != -1:
    formatAddress = formatAddress.replace('LOWR', '')
if formatAddress.find('BACK') != -1:
    formatAddress = formatAddress.replace('BACK', '')
    formatAddress = re.sub('[^A-Za-z0-9 ]+', '', formatAddress)                         ## Remove all other special characters.
    formatAddressList.append(formatAddress)

rowCounter += 1
return formatAddressList                                                                ## Return a formatted address.

def zipFormat(inputZip):
####################################################################################
## Formats input zipcodes.
####################################################################################
rowCounter = 0
for row in inputZip:
    formatZip = ''.join(inputZip[rowCounter])
    formatZip = formatZip.strip()

    if not formatZip:                                                                   ## Initial Error checking to remove problematic addresses to a correction table.
        errorRowList.append(rowCounter)
    if formatZip[: 5].isdigit() == False:                                               ## Check if first five characters are digits.
        errorRowList.append(rowCounter)
        formatZip = re.sub('[^0-9]+', '', formatZip)                                    ## Remove all other special characters.
        formatZip = formatZip[: 5]                                                      ## Return only the first five characters.
        formatZipList.append(formatZip)                                                 ## Append the zip code to the zip code list.

rowCounter += 1
return formatZipList

def addressParse(inputAddress):                                                         ## Probably better to pass this a list instead of running the library for individual addresses.
####################################################################################
## Parses an input string using the python address parsing library from SwoopSearch.
## https://github.com/SwoopSearch/pyaddress
####################################################################################
ap = AddressParser()                                                                    ## Initialize the address parser libary.
address = ap.parse_address(inputAddress) ## Pass an address to the address parser library.
## Since each parsed value needs to be modified in the same format, there may be a way to iterate through
## each returned value. However, the below method in comments does not work and will require further research.
##
## parsefieldList = ['house_number','street_prefix','address.street','street_suffix', 'apartment']
##
## for item in parsefieldList:
## strHouseNumber
## currentItem = parsefieldList.index(item)
## currentItem = str(address.currentItem).strip().upper()
## currentItem = re.sub('[^A-Za-z0-9 ]+', '', currentItem)
if address.house_number is not None: ##Check if house_number returns a value.
    strHouseNumber = str(address.house_number).strip().upper()                          ## Strip whitespace and change to uppercase.
    strHouseNumber = re.sub('[^A-Za-z0-9 ]+', '', strHouseNumber)                       ## Remove all special characters.
    
else: strHouseNumber = ""                                                               ## If house_number is None, then change to a blank string.
if address.street_prefix is not None:
    strStreetPrefix = str(address.street_prefix).strip().upper()
strStreetPrefix = re.sub('[^A-Za-z0-9 ]+', '', strStreetPrefix)
else :
    strStreetPrefix = ""
if address.street is not None: strStreet =
    str(address.street).strip().upper() strStreet = re.sub('[^A-Za-z0-9]+', '', strStreet)
else :strStreet = ""
if address.street_suffix is not None: strStreetSuffix = str(address.street_suffix).strip().upper()
strStreetSuffix = re.sub('[^A-Za-z0-9 ]+', '', strStreetSuffix)
else :
    strStreetSuffix = ""
if address.apartment is not None: strApartment =
    str(address.apartment).strip().upper() strApartment =
    re.sub('[^A-Za-z0-9 ]+', '', strApartment)
else :strApartment = ""
return (strHouseNumber, strStreetPrefix, strStreet, strStreetSuffix,
    strApartment) 
    
def addFields(featureClassLocation):                                                    ## Check to see if field names don 't exist. If they don' t exist, create them.
    
for entry in columnList:                                                                ## Runs a for loop based on a set of column name lists.
    columnHeader = ''.join(columnList[columnList.index(entry)]) 
    
    if not arcpy.ListFields(featureClassLocation, columnHeader):
        arcpy.AddField_management(featureClassLocation, columnHeader, "TEXT","", "", "60") 
    
    if outParsedRows == True:                                                           ## Create a second series of columns if parsed rows are requested.
    
    for entry in approvedParseList:
        columnHeader = ''.join(approvedParseList[approvedParseList.index(entry)]) 
        
        if not arcpy.ListFields(featureClassLocation, columnHeader):
            arcpy.AddField_management(featureClassLocation, columnHeader, "TEXT","", "", "60")
        
def geocodeAddress(inputTable, inputAddressLocator,outputGeocodeLocation):
####################################################################################
## Geocodes an input address string using the input Address Locator.
####################################################################################
arcpy.GeocodeAddresses_geocoding(inputTable, inputAddressLocator, "Address ADDRESS VISIBLE NONE;Zip ZIPCODE VISIBLE NONE", outputGeocodeLocation)

def spatialJoin(joinFeature, outputFeatureClass):
####################################################################################
## Spatially joins a set of geocoded addresses to a specified file.
####################################################################################
if joinFeature in approvedJoinList:
    if joinFeature == "2010 Census Block":
    targetDataset = r "C:\GIS\485\Final Project\BackgroundData.gdb\Rock_County_2010_Census_Block"
elif joinFeature == "2010 Census Block Group":
    targetDataset = r "C:\GIS\485\Final Project\BackgroundData.gdb\Rock_County_2010_Census_Block_Group"
else :
    arcpy.AddError("Error, join target not present in approved join list.")
return

joinOperation = 'JOIN_ONE_TO_ONE'
joinType = 'KEEP ALL'
fieldMappings = ''
matchOption = 'INTERSECT'
searchRadius = ''
distanceFieldName = ''
arcpy.SpatialJoin_analysis(targetDataset, joinFeature, outputFeatureClass, joinOperation, joinType, fieldMappings, matchOption, searchRadius, distanceFieldName)

main()