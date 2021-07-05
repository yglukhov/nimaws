#[
  # AwsClient

  The core library for building AWS service APIs
  implements:
    * an AwsClient which using an AsyncHttpClient for comm
    * a contructor function for the AwsClient that takes credentials and service scope
    * a request proc which takes an AwsClient and the request params to handle Sigv4 signing and async dispatch
 ]#

import times, tables, unicode,uri
import strutils except toLower
import httpclient, asyncdispatch
import sigv4

export sigv4.AwsCredentials, sigv4.AwsScope

const
  awsEndpt* = "amazonaws.com"
  defRegion* = "us-east-1"
type
  EAWSCredsMissing = object of Exception
  AwsRequest* = tuple
    action: string
    url: string
    payload: string

  AwsClient* {.inheritable.} = ref object
    httpClient*: AsyncHttpClient
    credentials*: AwsCredentials
    scope*: AwsScope
    endpoint*:Uri
    isAWS*:bool
    key*: string
    key_expires*: Time

const iso_8601_aws = "yyyyMMdd'T'HHmmss'Z'"

proc getAmzDateString*():string=
  return format(utc(getTime()), iso_8601_aws)

proc newAwsClient*(credentials:(string,string),region,service:string):AwsClient=
  let
    creds = AwsCredentials(credentials)
    # TODO - use some kind of template and compile-time variable to put the correct kernel used to build the sdk in the UA?
    httpclient = newAsyncHttpClient("nimaws-sdk/0.1.1; "&defUserAgent.replace(" ","-").toLower&"; darwin/16.7.0")
    scope = AwsScope(date:getAmzDateString(),region:region,service:service)

  return AwsClient(httpClient:httpclient, credentials:creds, scope:scope,key:"", key_expires:getTime())

proc request*(client: AwsClient,params:Table):Future[AsyncResponse]=
  let
    action = params.getOrDefault("action", "GET")
    payload = params.getOrDefault("payload")
    path = params.getOrDefault("path")
    bucket = params.getOrDefault("bucket")
    acl = params.getOrDefault("acl")

  if not client.httpClient.isNil:
    client.httpClient.close()
  client.httpclient = newAsyncHttpClient("nimaws-sdk/0.1.1; "&defUserAgent.replace(" ","-").toLower&"; darwin/16.7.0")

  var
    url:string

  if client.credentials.id.len == 0 or client.credentials.secret.len == 0:
    raise newException(EAWSCredsMissing,"Missing credentails id/secret pair")

  if client.isAws:
    if bucket.len != 0:
      url = ("https://$1.$2.amazonaws.com/$3" % [bucket,client.scope.service,path])
    else:
      url = ("https://$1.amazonaws.com/$2" % [client.scope.service,path])
  else:
     if client.endpoint.port.len > 0 and client.endpoint.port != "80":
        url = ("$1://$2:$3/$4$5" % [client.endpoint.scheme,client.endpoint.hostname,client.endpoint.port,bucket,path])
     else:
        url = ("$1://$2/$3$4" % [client.endpoint.scheme,client.endpoint.hostname,bucket,path])
  let
     req:AwsRequest = (action: action, url: url, payload: payload)

  # echo "url: ", url
  # Add signing key caching so we can skip a step
  # utilizing some operator overloading on the create_aws_authorization proc.
  # if passed a key and not headers, just return the authorization string; otherwise, create the key and add to the headers
  client.httpClient.headers.clear()
  if client.key_expires <= getTime():
    client.scope.date = getAmzDateString()
    client.key = create_aws_authorization(client.credentials, req, client.httpClient.headers.table, client.scope)
    client.key_expires = getTime() + minutes(5)
  else:
    let auth = create_aws_authorization(client.credentials[0], client.key, req, client.httpClient.headers.table, client.scope)
    client.httpClient.headers.add("Authorization", auth)

  if acl.len > 0 :
    client.httpClient.headers.add("x-amz-acl", acl)

  return client.httpClient.request(url,action,payload)
