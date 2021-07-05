#[
  # S3Client

  A simple object API for performing (limited) S3 operations
 ]#

import strutils except toLower
import times, unicode, tables, asyncdispatch, httpclient, streams, os, strutils, uri, xmltree, xmlparser
import awsclient


type
  S3Client* = ref object of AwsClient

proc newS3Client*(credentials:(string,string),region:string=defRegion,host:string=awsEndpt): S3Client =
  let
    creds = AwsCredentials(credentials)
    # TODO - use some kind of template and compile-time variable to put the correct kernel used to build the sdk in the UA?
    httpclient = newAsyncHttpClient("nimaws-sdk/0.1.1; "&defUserAgent.replace(" ","-").toLower&"; darwin/16.7.0")
    scope = AwsScope(date: getAmzDateString(), region: region, service: "s3")
  var endpoint:string
  if not host.startsWith("http"):
    endpoint = "https://" & host
  else:
    endpoint = host
  return S3Client(httpClient:httpclient, credentials:creds, scope:scope, endpoint:parseUri(endpoint),isAWS:endpoint.endsWith(awsEndpt),key:"", key_expires:getTime())

# Getting objects
proc getObjectRaw(self: S3Client,bucket,key:string): Future[AsyncResponse] =
  var
    path = key
  let params = {
        "bucket":bucket,
        "path": path
      }.toTable

  return self.request(params)

proc getObjectContentAux(self: S3Client, bucket, key: string): Future[string] {.async.} =
  let r = await self.getObjectRaw(bucket, key)
  result = await r.body
  if r.contentLength != result.len:
    raise newException(Exception, "Wrong content length. Expected: " & $r.contentLength & ", got: " & $result.len)

proc getObjectContent*(self: S3Client, bucket, key: string): Future[string] {.async.} =
  var ok = false
  for i in 0 ..< 5:
    try:
      result = await getObjectContentAux(self, bucket, key)
      ok = true
      break
    except:
      discard

  if not ok:
    raise newException(IOError, "Could not get file " & key)

proc downloadObjectIfAbsent*(self: S3Client, bucket, key, toPath: string) {.async.} =
  if not fileExists(toPath):
    let c = await self.getObjectContent(bucket, key)
    writeFile(toPath, c)

# Putting objects
## put_object
##  bucket name
##  path has to be absoloute path in the form /path/to/file
##  payload is binary string
proc putObjectRaw(self: S3Client, bucket, path, payload, acl: string): Future[AsyncResponse] =
  let params = {
      "action": "PUT",
      "bucket": bucket,
      "path": path,
      "payload": payload,
      "acl": acl
    }.toTable

  return self.request(params)

proc listObjectsRaw*(self: S3Client, bucket: string, prefix = ""): Future[AsyncResponse] =
  var params = {
      "bucket": bucket
    }.toTable
  if prefix.len != 0: params["prefix"] = prefix
  return self.request(params)

proc parseListingSimple(listing, prefix: string): seq[tuple[path: string, size: int64]] =
  let x = parseXml(listing)
  for c in x:
    if c.tag == "Contents":
      let k = c.child("Key").innerText
      if prefix.len == 0:
        let sz = parseBiggestInt(c.child("Size").innerText)
        result.add((k, sz))
      elif k.startsWith(prefix) and k.len != prefix.len:
        let path = k[prefix.len .. ^1]
        let sz = parseBiggestInt(c.child("Size").innerText)
        result.add((path, sz))

proc parseAsyncListingSimple(f: Future[AsyncResponse], prefix: string): Future[seq[tuple[path: string, size: int64]]] {.async.} =
  let r = await f
  if r.code != Http200:
    raise newException(IOError, "Bad listing result: " & r.status)
  let body = await r.body
  return parseListingSimple(body, prefix)

proc listObjectsSimple*(self: S3Client, bucket: string, prefix = ""): Future[seq[tuple[path: string, size: int64]]] =
  parseAsyncListingSimple(self.listObjectsRaw(bucket, prefix), prefix)

proc listBucketsRaw*(self: S3Client): Future[AsyncResponse] =
  let params = {
      "action": "GET"
    }.toTable

  return self.request(params)

proc getAclRaw*(self: S3Client, bucket: string): Future[AsyncResponse] =
  let params = {
      "action": "GET",
      "bucket": bucket,
      "path": "/?acl=",
      # "path": "/some-folder/somefile-1194"
    }.toTable

  return self.request(params)

proc putObjectWithContent*(self: S3Client, content, bucket, key: string, acl: string = "") {.async.} =
  let r = await self.putObjectRaw(bucket, key, content, acl)
  let b = await r.body
  if r.code != Http200:
    raise newException(IOError, "Could not put file " & key)

proc putObject*(self: S3Client, localPath, bucket, key: string): Future[void] =
  putObjectWithContent(self, readFile(localPath), bucket, key)

# Deleting objects
proc deleteImpl(f: Future[AsyncResponse]) {.async.} =
  let r = await f
  if r.code notin { Http200, Http204 }:
    raise newException(IOError, "Could not delete file")

proc deleteObjects*(self: S3Client, bucket: string, keys: openarray[string]): Future[void] =
  let n = newElement("Delete")
  for k in keys:
    let xk = newElement("Key")
    xk.add(newText(k))
    n.add(newXmlTree("Object", [xk]))

  let params = {
      "bucket": bucket,
      "payload": $n,
    }.toTable

  return deleteImpl(self.request(params))

proc deleteObject*(self: S3Client, bucket: string, key: string): Future[void] =
  var key = key
  if key[0] != '/': key = '/' & key
  let params = {
      "action": "DELETE",
      "bucket": bucket,
      "path": key
    }.toTable

  return deleteImpl(self.request(params))
