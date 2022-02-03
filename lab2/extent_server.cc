// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() 
{
    im = new inode_manager();
}

int extent_server::create(int clt, uint32_t type, extent_protocol::extentid_t &id) {
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

int extent_server::put(int clt, extent_protocol::extentid_t id, std::string buf, int &) {
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  std::cout << "writing = " << buf << std::endl;
  return extent_protocol::OK;
}

int extent_server::get(int clt, extent_protocol::extentid_t id, std::string &buf) {
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(int clt, extent_protocol::extentid_t id, extent_protocol::attr &a) {
//  printf("extent_server: getattr %lld\n", id);
  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(int clt, extent_protocol::extentid_t id, int &)
{
//  printf("extent_server: write %lld\n", id);
  id &= 0x7fffffff;
  im->remove_file(id);
  return extent_protocol::OK;
}

int extent_server::setattr(int clt, extent_protocol::extentid_t id, short type, unsigned int atime, unsigned int mtime, unsigned int ctime, int &) {
//    printf("extent_server: setattr %lld\n", id);
    id &= 0x7fffffff;
    im->setattr(id, type, atime, mtime, ctime);

    return extent_protocol::OK;
}