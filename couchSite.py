#!/usr/bin/env python
"""
Upload a site directory structure to a
couch database.

Use --help to see options.
"""

import sys, os, traceback
import json, couchdb

# {{{ CouchUploader
class CouchUploader():
  """Performs the upload
  """

  def __init__(self, couchDB_URL, databaseName):
    self.couchDB_URL=couchDB_URL
    self.databaseName=databaseName

    self.couch = couchdb.Server(self.couchDB_URL)

    try:
        self.db = self.couch[self.databaseName]
    except couchdb.ResourceNotFound:
        self.db = self.couch.create(self.databaseName)



  def uploadDirectoryToDocument(self,directory,documentID):
    """Walk through directory for files and copy
    them into the database as attachments to the given document
    Deleting what was there before.
    Directories paths become attachment 'filenames' so there is
    a flat list like the output of the 'find' command.
    """

    print("uploading ", directory, " to ", documentID, " of ", self.databaseName)

    # find the database and delete the .site related
    # documents if they already exist

    # create the document
    document = self.db.get(documentID)
    if document:
      self.db.delete(document)
    documentJSON = {
        '_id' : documentID,
        'fromDirectory' : directory
      }
    self.db.save(documentJSON)

    # put the attachments onto the document
    for root, dirs, files in os.walk(directory):
        for fileName in files:
            if fileName.startswith('.'):
                continue
            fileNamePath = os.path.join(root,fileName)
            try:
                relPath = os.path.relpath(fileNamePath, directory)
                fp = open(fileNamePath, "rb")
                self.db.put_attachment(documentJSON, fp, relPath)
                fp.close()

            except Exception as e:
                print("Couldn't attach file %s" % fileNamePath)
                print(str(e))
                traceback.print_exc()
                continue

  def uploadDesignDocuments(self,directory):
    """
    For each python file in the directory create a design document based
    on the filename containing the json formatted views (map reduce
    javascript functions).
    """

    import glob
    pattern = os.path.join(directory,'*.py')
    viewFiles = glob.glob(pattern)
    for viewFile in viewFiles:
        exec(compile(open(viewFile, "rb").read(), viewFile, 'exec'))
        for view in views:
            viewID = os.path.join('_design',view)
            print("uploading ", view, " of ", viewFile, " to ", self.databaseName)
            document = self.db.get(viewID)
            if document:
                self.db.delete(document)
            self.db[viewID] = views[view]


# }}}

# {{{ main, test, and arg parse

def usage():
    print("couchSite [siteDirectory] <DatabaseName>")
    print("couchSite [siteDirectory] <CouchDB_URL> <DatabaseName>")
    print(" CouchDB_URL default http:localhost:5984")
    print(" DatabaseName default dicom_search")

def main ():

    couchDB_URL='http://localhost:5984'
    databaseName='test'
    sitePath = sys.argv[1]

    if len(sys.argv) == 3:
        databaseName = sys.argv[2]
    if len(sys.argv) > 3:
        couchDB_URL = sys.argv[2]
        databaseName = sys.argv[3]

    uploader = CouchUploader(couchDB_URL, databaseName)
    uploader.uploadDesignDocuments(os.path.join(sitePath,"design"))
    uploader.uploadDirectoryToDocument(os.path.join(sitePath,"site"), ".site")

forIPython = """
import sys
sys.argv = ('test', '/Users/pieper/Downloads/dicom/DICOMSearch')
"""

if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            raise BaseException('missing arguments')
        main()
    except Exception as e:
        print('ERROR, UNEXPECTED EXCEPTION')
        print(str(e))
        traceback.print_exc()

# }}}

# vim:set sr et ts=4 sw=4 ft=python fenc=utf-8: // See Vim, :help 'modeline
# vim: foldmethod=marker
