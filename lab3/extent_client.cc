// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>


extent_client::extent_client(std::string dst)
{
    printf("hello\n");
    sockaddr_in dstsock;
    printf("hello\n");
  make_sockaddr(dst.c_str(), &dstsock);
    printf("hello\n");

  cl = new rpcc(dstsock);
    printf("hello\n");
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
    printf("hello\n");
//=======
//extent_client::extent_client() {
//  es = new extent_server();
//>>>>>>> lab1
}

extent_protocol::status extent_client::create(uint32_t type, extent_protocol::extentid_t &id) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    ret = cl->call(extent_protocol::create, cl->id(), type, id);
    VERIFY (ret == extent_protocol::OK);
    return ret;
}

extent_protocol::status extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    ret = cl->call(extent_protocol::get, cl->id(), eid, buf);
    return ret;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    ret = cl->call(extent_protocol::getattr, cl->id(), eid, attr);
    return ret;
}

extent_protocol::status extent_client::put(extent_protocol::extentid_t eid, std::string buf) {
    extent_protocol::status ret = extent_protocol::OK;
    int useless_return = 0;
    ret = cl->call(extent_protocol::put, cl->id(), eid, buf, useless_return);
  // Your lab2 part1 code goes here
//=======
//  int r;
//  printf("extent_client start put\n");
//  ret = es->put(eid, buf, r);
//    printf("extent_client end put\n");
//>>>>>>> lab1
    return ret;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    int useless_int = 0;
    ret = cl->call(extent_protocol::remove, cl->id(), eid, useless_int);
    return ret;
//<<<<<<< HEAD
}
//=======
//}
//
extent_protocol::status extent_client::setattr(extent_protocol::extentid_t eid, short type, unsigned int atime, unsigned int mtime, unsigned int ctime) {
    extent_protocol::status ret = extent_protocol::OK;
    int useless_int = 0;
    ret = cl->call(extent_protocol::setattr, cl->id(), eid, type, atime, mtime, ctime, useless_int);
//    int r;
//    ret = es->setattr(eid, type, atime, mtime, ctime);
    return ret;
}
//>>>>>>> lab1
