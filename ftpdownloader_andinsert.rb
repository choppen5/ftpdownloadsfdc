require 'net/ftp'
require 'csv'
require 'logger'
require 'fileutils'
require 'mail'
require 'salesforce_bulk_quickfix'
load './config.rb' 

#setup:
#1.  setup ftp users name,use command line to: "export FTPDOWNLOADER_USER=youruser, FTPDOWNLOADER_PASSWORD=yourpassword" environment variables.. or hard code ftpuser below. 
#2.  create processedfiles folder

####config
## ftp stuff -
$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

$log.debug("Created logger")

## sfdc options
testfile = nil # "csvtest.csv" #set this to real filename to skip FTP files, otherwise NULL to use FTP
#testfile = "csvtest.csv"

#for logging into sandbox
sfdcsandbox =  true

mailrecepients = "choppen5@gmail.com,choppen5@yahoo.com"

##methods
def downloadfile 
    $log.info("FTPDownloader started")

    #todo: check null values, error if these env aren't set
    
    unless $ftpuser && $ftppassword 
        throw "missing ftpuser && ftppassword"
    end

    dir = "/private/dataexport/" 


    #outputfile creationstuff 
    # test or create these dirs?
    ftpdownloadir = "ftpdownload"
    cleaneddir = "processedfiles/"
    downloadfilepath = "" #variable for the latest file

    #starterup.. ftp login
    ftp = Net::FTP::new($ftphost)
    ftp.login($ftpuser,$ftppassword)
    #should check and log errors here

    ftp.chdir(dir)
    fileList = ftp.list

    latestfile = ""
    filehash = Hash.new

    fileList.each do |file|
      puts "file = #{file}"
      date = Array.new
      #parse windows ftp format: 03-16-13  03:35PM             28172185 DBExportToCSV_20130316.csv
  
      date = /(\d+)\-(\d+)\-(\d+)\s+(\d+):(\d+)(\w+)/.match(file)
  
      yeardate = /CSV_(\d+)\.csv/.match(file)
      filename = /(\s\w+_\d+\.csv)/.match(file)
  
      if yeardate #matches file format
        #this file format contains date in filename YYYYMMDD which we will use to sort
        filehash[yeardate[1]] = file 
      end
  
      if filename #filename .csv
        latestfile = filename[1]
      end
  
    end

    #sort by date
     filehash.sort.reverse.map do |key,value|
       puts "sorted, latest file: #{key} = #{value}"
       filename = /(\w+_\d+\.csv)/.match(value)
   
       if filename
         latestfile = filename[1]
       end
   
       downloadfilepath = ftpdownloadir + latestfile 
       $log.info("downloadfile = #{downloadfilepath}")
   
       ftp.gettextfile(latestfile)
       FileUtils.mv latestfile, downloadfilepath
       break  #only get first file (sorted)
   
     end
    ftp.close
    $log.info("sucussfully downloaded and moved file #{downloadfilepath}")
    return downloadfilepath
end

def transform_row_contact (row)
    
    contact_hash = {"Screener_ID__c" => row[0],
                     "Screener_GUID__c" => row[1],
                     "FirstName" => row[2],
                     "LastName" => row[3],
                     "Email" => row[4],
                     "Caller_Type__c" => "Other"                                        
                  }
end

def transform_row_case (row)
  case_hash   =  { "Screener_GUID__c" => row[1],
                   "Score__c" => row[12],
                    "Call_Results__c" => "D4C Pop Screener",
                   "Origin" => "Screener",
                   "Subject" => "Screener Import",
                   "Question_Five__c" => row[10],
                   "Question_Four__c" => row[9],
                   "Question_Three__c" => row[8],
                   "Question_Two__c" => row[7],
                   "Question_One__c" => row[6],
                   "Status" => "Closed"                                       
                  }
end

#starterup

salesforce = SalesforceBulk::Api.new($sfdcuser, $sfdcpassword, sfdcsandbox)
$log.debug("Logged into Salesforce")


#open up history file, get value from last row processed
#lastline = File.open("historyfile").last.split(',')[0].to_i
lastline = nil
File.open("historyfile").each do |line|
    lastline = line if(!line.chomp.empty?)
    # Do all sorts of other things
end
if(lastline)
  lastline = lastline.split(',')[0].to_i
    # Do things with the last non-empty line.
end

$log.debug("Last line processed last run = #{lastline}")


#csv setup
headers = true

file = ""
#openfile
if testfile
  $log.info("Testfile = #{testfile}") 
  file = testfile
else
  $log.info("Retrieving FTP file...")
  file = downloadfile
end

#prepare CSV for 

timestamp = Time.now.to_i
discardfilename = "SurveyResultsWithoutName#{timestamp}From" + file
cleaneddir = "processedfiles/"
discardfilepath = cleaneddir + discardfilename
discardfile = File.new(discardfilepath, 'w')



timestamp = Time.now.to_i



$log.info("Dedupping #{file}")

uniquecontacts = Hash.new
contactarray = Array.new
casearray = Array.new

linenumber = 0 
discardedcontacts = 0 #those that don't have lastname + email
linecount = 0 # for storing number of lines (past the last run)

CSV.open(file, :headers => headers, :return_headers => true).each do |row|
   
    
     if row.header_row?
        discardfile.write(row)
        next  #skip headers
     end
     
    linenumber = row[0].to_i #csv file format has a line count as the first entry
     
    $log.debug("checking linenumber: #{linenumber} <= lastline: #{lastline}")
    #check if we already processed this in history
    if linenumber <= lastline
      $log.debug("Skipping line with number   #{row[0].to_i}")
      next
    end
    #check last row proccessed
    
    linecount += 1 #first non header row we are going to process
    
    if (row[3] && row[4] )  #has lastname and email
      
        contact = transform_row_contact(row)
        caserow = transform_row_case(row)
        contactarray.push(contact)
        casearray.push(caserow)      
  
    else
      puts "Found result without lastname + email #{row}"
      discardedcontacts += 1
      discardfile.write(row)
      puts "column 1 = #{row[1]}"
    end
      
      
end

$log.info("Writing last processed into historyfile: #{linenumber}")

File.open("historyfile", 'a') do |file|
  file.puts "#{linenumber},#{Time.now}"
end

$log.info("Found #{contactarray.count} new contacts in this file")

if contactarray.count > 0
  #SFDC call - do insert and wait for results
  $log.info("Inserting #{contactarray.count} contacts")
  result = salesforce.create("Contact", contactarray,true)

    ## check for errors? 

    #puts "results: "  + result.inspect
    resultrecords = result.result.records #sucessfull records...array that looks like this: 003J000000fqKPCIA2,true,true,"" 

    count = 0

    contactarray.each { |contact|  
      contactid = resultrecords[count][0]  #resultrecords[count][0] matches the result record to the input array.  
      puts "contact id = #{contactid}"
      contactarray[count]["ContactId"] = contactid #added a hash key "ContactId"    
      count += 1
    }

    # todo: write results to csv

    $log.info("finished processing inserted contacts and matching results")


    $log.info("now updating cases with contactid")
    updatedcases = Array.new
    count = 0 #reset
    contactarray.each { |contact|
        contactGUid = contact["Screener_GUID__c"]  #match case array by guid
        contactid =   contact["ContactId"]         #created from inserting contacts
    
        foundcase = Array.new
        foundcase =  casearray.select {|caserow| caserow["Screener_GUID__c"] == contactGUid }.first  #pull out matching case for this contact, grab first in array    
        foundcase["ContactId"] = contactid  #add ContactId to case before inserting   
        $log.debug("Updated case, ready to insert: " + foundcase.inspect)
        updatedcases.push(foundcase)  
    }

    $log.info("Calling SFDC insert for Case")
    #SFDC call - do insert and wait for results
    
    caseresult = salesforce.create("Case", updatedcases,true)

    $log.info("Completed SFDC insert for Case")
    puts "Case results = " + caseresult.inspect
end


$log.info("Finished File processing.. sending email")
discardfile.close

Mail.defaults do
  delivery_method :smtp, $mailoptions
end

Mail.deliver do
       to mailrecepients
     from 'copdfoundationscreener@gmail.com'
  subject 'COPD Screener import results'
     body "This run proccessed #{linecount} surveys since last run.\nFound #{contactarray.count} new contacts + surveys to insert into SFDC for this run.\nContacts without lastname + email= #{discardedcontacts}\n"
     add_file discardfilepath
end


#file downloaded, moved into FTPdowlnloadir
#now, process it:



