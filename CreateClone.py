#!/usr/bin/env python
#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: mlohani@nutanix.com
#
# A simple python script to demonstrate how to crate clones of a VM using Acropolis REST APIs
#
# In clone mode (--action=clone), this script creates --num_clones number 
# of clones of a VM (identified by --vm_name) from a specified Nutanix cluster
# (--cluster_ip).
#
# In cleanup mode (--action=cleanup), this script deletes all clones of a VM 
# with names starting with --vm_name from a specified Nutanix cluster
# (--cluster_ip).
#
# Prerequisites:
#  - The host that runs this script must have autofs /net enabled.
#  - The host that runs this script must be added to the cluster's NFS
#    whitelist.
#

import errno
import gflags
import json
import os
import requests
import shutil
import traceback
import urllib
import uuid
import time

gflags.DEFINE_string("action", None,
                     "Action ('clone' or 'cleanup')")

gflags.DEFINE_string("cluster_ip", None,
                     "Cluster IP address (CVM or virtual IP)")

gflags.DEFINE_string("username", "admin", "Prism username")

gflags.DEFINE_string("password", "admin", "Prism password")

gflags.DEFINE_string("vm_name", None, "VM name")

gflags.DEFINE_integer('num_clones', 1, 'Number Of Clones')

FLAGS = gflags.FLAGS

class RestApiClient():

  def __init__(self, cluster_ip, username, password):
    """
    Initializes the options and the logfile from GFLAGS.
    """
    self.cluster_ip = cluster_ip
    self.username = username
    self.password = password
    self.base_acro_url = (
        "https://%s:9440/api/nutanix/v0.8" % (self.cluster_ip,))
    self.base_pg_url = (
        "https://%s:9440/PrismGateway/services/rest/v1" % (self.cluster_ip,))
    self.session = self.get_server_session(self.username, self.password)

  def get_server_session(self, username, password):
    """
    Creating REST client session for server connection, after globally setting
    Authorization, Content-Type and charset for session.
    """
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False
    session.headers.update(
        {'Content-Type': 'application/json; charset=utf-8'})
    return session

  def _url(self, base, path, params):
    """
    Helper method to generate a URL from a base, relative path, and dictionary
    of query parameters.
    """
    if params:
      return "%s/%s?%s" % (base, path, urllib.urlencode(params))
    else:
      return "%s/%s" % (base, path)

  def acro_url(self, path, **params):
    """
    Helper method to generate an Acropolis interface URL.
    """
    return self._url(self.base_acro_url, path, params)

  def pg_url(self, path, **params):
    """
    Helper method to generate an Prism Gateway interface URL.
    """
    return self._url(self.base_pg_url, path, params)

  def resolve_vm_uuid(self, vm_name, check_unique):
    """
    Resolves a VM name to a UUID. Fails if the name is not found, or not
    unique.
    """
    # Use prism gateway interface to do a filtered query.
    url = self.pg_url("vms", filterCriteria="vm_name==" + vm_name)
    r = self.session.get(url)
    if r.status_code != requests.codes.ok:
      raise Exception("GET %s: %s" % (url, r.status_code))

    # Make sure we got one unique result.
    obj = r.json()
    count = obj["metadata"]["count"]
    if (check_unique):
      if count == 0:
        raise Exception("Failed to find VM named %r" % (vm_name,))
      if count > 1:
        raise Exception("VM name %r is not unique" % (vm_name,))

    # Prism likes to prepend the VM UUID with a cluster ID, delimited by "::".
    parts = obj["entities"][0]["vmId"].rsplit(":", 1)
    return parts[-1]

  def get_vm_info(self, vm_uuid):
    """
    Fetches the VM descriptor.
    """
    # Use acropolis interface to fetch the vm configuration.
    url = self.acro_url("vms/%s" % (vm_uuid,))
    r = self.session.get(url)
    if r.status_code != requests.codes.ok:
      raise Exception("GET %s: %s" % (url, r.status_code))
    return r.json()

  def poll_task(self, task_uuid):
    """
    Polls a task until it completes. Fails if the task completes with an error.
    """
    url = self.acro_url("tasks/%s/poll" % (task_uuid,))
    while True:
      print("Polling task %s for completion" % (task_uuid,))
      r = self.session.get(url)
      if r.status_code != requests.codes.ok:
        raise Exception("GET %s: %s" % (url, r.status_code))

      task_info = r.json()["taskInfo"]
      mr = task_info.get("metaResponse")
      if mr is None:
        continue
      if mr["error"] == "kNoError":
        break
      else:
        raise Exception("Task %s failed: %s: %s" %
            (task_uuid, mr["error"], mr["errorDetail"]))


  def _strip_empty_fields(self, proto_dict):
    """
    Removes empty spaces in a proto.
    TBD: fix to reduce complexity.
    """
    def strip_dict(d):
      if type(d) is dict:
        return dict((k, strip_dict(v))\
                    for k,v in d.iteritems() if v and strip_dict(v))
      if type(d) is list:
        return [strip_dict(v) for v in d if v and strip_dict(v)]
      else:
        return d
    return strip_dict(proto_dict)


  def construct_vm_clone_proto(self, vm_uuid, vm_info, num_clones):
    """
      speclist: List of vm_clone_protos, all other parameters will be ignored.
      numVCPUs": "integer",
      overrideNetworkConfig": "false"|"true",
      name": "string",
      memoryMb": "integer",
      uuid": "string",
      vmNics: List of nic_protos
      sourceVMLogicalTimestamp": "integer"
    """
    vm_clone_proto = {
         "numVcpus": vm_info["config"]["numVcpus"],
         "overrideNetworkConfig": "false",
         "name": vm_info["config"]["name"],
         "memoryMb": vm_info["config"]["memoryMb"],
         "uuid": "",
         "vmNics": [],
         "sourceVMLogicalTimestamp": ""
    }
    specs = []
    name = vm_clone_proto["name"]
    for i in range(0, num_clones):
      vm_clone_proto["name"] = name + "-" + str(i)
      print("Creating Clone %s of VM %s" % (i, vm_clone_proto["name"]))
      specs.append(self._strip_empty_fields(vm_clone_proto))
    return {"specList": specs}

  def create_clones(self, vm_uuid, vm_info, num_clones):
    # Prepare a VM clone spec.
    cloneSpec = self.construct_vm_clone_proto(vm_uuid, vm_info, num_clones)

    # Create VMs by cloning the given VM.
    url = self.acro_url("vms/"+str(vm_uuid)+"/clone")

    print ("CLONE START TIME", time.strftime("%H:%M:%S"))
    r = self.session.post(url, data=json.dumps(cloneSpec))
    if r.status_code != requests.codes.ok:
      raise Exception("POST %s: %s" % (url, r.status_code))
    task_uuid = r.json()["taskUuid"]
    self.poll_task(task_uuid)
    print ("CLONE END TIME", time.strftime("%H:%M:%S"))

  def cleanup_clones(self, vm_name, num_clones):
    for i in range(0, num_clones):
      to_be_deleted_vm_name = vm_name + "-" + str(i)
      print("Deleting VM %s" % (to_be_deleted_vm_name))
      to_be_deleted_vm_uuid = self.resolve_vm_uuid(to_be_deleted_vm_name, 0)
      url = self.acro_url("vms/"+str(to_be_deleted_vm_uuid))
      r = self.session.delete(url)
      if r.status_code != requests.codes.ok:
        raise Exception("DELETE %s: %s" % (url, r.status_code))
      task_uuid = r.json()["taskUuid"]
      self.poll_task(task_uuid)
       
def clone(c):
  assert(FLAGS.vm_name is not None)
  vm_uuid = c.resolve_vm_uuid(FLAGS.vm_name, 1)
  vm_info = c.get_vm_info(vm_uuid)
  c.create_clones(vm_uuid, vm_info, FLAGS.num_clones)

def cleanup(c):
  assert(FLAGS.vm_name is not None)
  c.cleanup_clones(FLAGS.vm_name, FLAGS.num_clones)

def main():
  assert(FLAGS.action is not None)
  assert(FLAGS.cluster_ip is not None)
  assert(FLAGS.num_clones is not None)
  c = RestApiClient(FLAGS.cluster_ip, FLAGS.username, FLAGS.password)
  if FLAGS.action == "clone":
    clone(c)
  elif FLAGS.action == "cleanup":
    cleanup(c)
  else:
    raise Exception("Unknown --action; expected 'clone' or 'cleanup'")

if __name__ == "__main__":
  import sys
  gflags.FLAGS(sys.argv)
  sys.exit(main())
