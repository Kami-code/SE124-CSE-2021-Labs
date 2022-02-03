#include "inode_manager.h"
#include <cstring>
#include <cstdio>
#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
using namespace std;


#define IndexLengthInIndirectBlock 5

int verbose = 0;

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf) {
    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf) {
    memcpy(blocks[id], buf, BLOCK_SIZE);
}


// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  verbose = false;
  if (verbose) printf("try to alloc block \n");
  if (verbose) printf("start number = %d, end_number = %d \n", IBLOCK(INODE_NUM + 1, BLOCK_NUM), BLOCK_NUM);

  for (int i = IBLOCK(INODE_NUM + 1, BLOCK_NUM); i < BLOCK_NUM; i++) {
      char read_bitmap_of_block_i[BLOCK_SIZE];
      read_block(BBLOCK(i), read_bitmap_of_block_i);

      //each block can place BPB bit,
      int bit_index = i - i/BPB * 4096;
      int char_index = bit_index / 8;
      int bit_index_in_char = bit_index - char_index * 8;
      //printf("\t 正在枚举第%d个block，它在bit_map_block中是第%d位，在第%d个字符中，是第%d位 \n", i, bit_index, char_index, bit_index_in_char);


      unsigned char corresponding_char = read_bitmap_of_block_i[char_index];
      unsigned char result = (((corresponding_char << (bit_index_in_char )) & 0xff) >> 7);
      //printf("\t corresponding_int = %d, result = %d \n", (int)corresponding_char, (int)result);
      if (result == 1) {
          continue;
      }
      else if (result == 0) {
          corresponding_char |= (128 >> (bit_index_in_char));
          if (verbose)printf("\t after_modified_corresponding_int = %d\n", (int)corresponding_char);
          read_bitmap_of_block_i[char_index] = (char)corresponding_char;
          write_block(BBLOCK(i), read_bitmap_of_block_i);
          if (verbose)printf("\t going to alloc block number =  %d \n", i);
          char tmp[BLOCK_SIZE];
          unsigned char cool;
          read_block(BBLOCK(i), tmp);

          cool = tmp[char_index];

          result = (((cool << (bit_index_in_char)) & 0xff) >> 7);
          if (verbose)printf("\t reloaded target char = %d, bit = %d \n", (int)cool, (int)result);
          assert(cool == corresponding_char);
          assert(result == 1);
          verbose = true;

          return i;
      }
      else {
          exit(0);
      }
  }
  return -1;
}

void block_manager::free_block(uint32_t id) {
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
    printf("freeing block = %d \n", id);
    if (id > BLOCK_NUM) return;
    char read_bitmap_of_block_id[BLOCK_SIZE];
    read_block(BBLOCK(id), read_bitmap_of_block_id);
    //each block can place BPB bit,
    int bit_index = id - id/BPB * 4096;
    int char_index = bit_index / 8;
    int bit_index_in_char = bit_index - char_index * 8;
    char corresponding_char = read_bitmap_of_block_id[char_index];
    int result = (((corresponding_char << (bit_index_in_char)) & 0xff) >> 7);

    corresponding_char &= (~(128 >> (bit_index_in_char)));
    read_bitmap_of_block_id[char_index] = corresponding_char;
    write_block(BBLOCK(id), read_bitmap_of_block_id);
    return;

}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
  d = new disk();

  // format the disk
  // superblock 1
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
    char all_zero_string[BLOCK_SIZE];
    for (int i = 0; i < BLOCK_SIZE; i++) {
        all_zero_string[i] = 0;
    }
  for (int block_index = 0; block_index < BLOCK_NUM; block_index++) {
      write_block(BBLOCK(block_index), all_zero_string);
  }
}

void block_manager::read_block(uint32_t id, char *buf) {
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf) {
  d->write_block(id, buf);
}

// inode layer -----------------------------------------
inode_manager::inode_manager() {
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
  else {
      printf("\t successfully create root_dir when init! \n");
      extent_protocol::attr a;
      getattr(1, a);
      printf("rootdir size = %d, type = %d, atime = %d, ctime = %d, mtime = %d", a.size, a.type, a.atime, a.ctime, a.mtime);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  if (verbose) printf("alloc_inode \n");
  inode* tmp_inode_saver;
  for (uint32_t i = 1; i < INODE_NUM; i++) {
      tmp_inode_saver = get_inode(i);
      if (tmp_inode_saver == NULL) {
          tmp_inode_saver = (struct inode*)malloc(sizeof(struct inode));
          tmp_inode_saver->type = type;
          tmp_inode_saver->size = 0;
          put_inode(i, tmp_inode_saver);
          free(tmp_inode_saver);
          return i;
      }
  }
  exit(0);
  return -1;
}

void inode_manager::free_inode(uint32_t inum) {
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
    printf("enter free_inode \n");
    inode* tmp_inode_saver = get_inode(inum);
    if (tmp_inode_saver == NULL) {
        return;
    }
    else {
        for (int i = 0; i < NDIRECT; i++) {
            blockid_t current_block_number = tmp_inode_saver->blocks[i];
            printf("\t enum block = %d \n", tmp_inode_saver->blocks[i]);
            if (tmp_inode_saver->blocks[i] != 0) {
                if ((tmp_inode_saver->blocks[i] <= BLOCK_NUM) && (tmp_inode_saver->blocks[i] >= 1)) {
                    bm->free_block(tmp_inode_saver->blocks[i]);
                }
                tmp_inode_saver->blocks[i] = 0;
            }
            else {
                break;
            }
        }
        if (tmp_inode_saver->blocks[NDIRECT] != 0) {
            //todo:handle memory leak here
//            int indirect_block_cursor = 0;
//            char indirect_block_data[BLOCK_SIZE];
//            bm->read_block(tmp_inode_saver->blocks[NDIRECT], indirect_block_data);
//            while(1) {
//                char current_direct_block_number_string[IndexLengthInIndirectBlock];
//                memcpy(current_direct_block_number_string, indirect_block_data + indirect_block_cursor * IndexLengthInIndirectBlock, IndexLengthInIndirectBlock);
//                if (atoi(current_direct_block_number_string) == -1) break;
//                else {
//                    blockid_t current_direct_block_number = atoi(current_direct_block_number_string);
//                    indirect_block_cursor++;
//                    bm->free_block(current_direct_block_number);
//                }
//            }
            bm->free_block(tmp_inode_saver->blocks[NDIRECT]);
            tmp_inode_saver->blocks[NDIRECT] = 0;
        }

        tmp_inode_saver->type = 0;
        put_inode(inum, tmp_inode_saver);
        free(tmp_inode_saver);
    }
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  //printf("\tim: get_inode %d\n", inum);

  if (inum < 1 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf); // Block containing inode i
  // printf("%s:%d\n", __FILE__, __LINE__);
  ino_disk = (struct inode*)buf + inum % IPB; //change buf to inode_dist
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }
  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk; //deep copy and return
  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  //printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  //printf("\tim: put_inode over\n");
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
    verbose = true;
//    if(verbose) printf("start read_file \n");
    inode * current_inode = get_inode(inum);
    if (current_inode == NULL) {
        return;
    }

    *size = current_inode->size;
    int remaining_size = current_inode->size;
    int current_block_cursor = 0;
    int done_size = 0;
    char tmp_block_data[BLOCK_SIZE];
//    if (verbose) printf("current inode number = %d, inode_size = %d \n", inum, current_inode->size);

    char* result_string = (char*) malloc(sizeof(char) * (current_inode->size + 1));
    result_string[current_inode->size] = 0;
    *buf_out = result_string;

    while ((current_block_cursor < NDIRECT) && (remaining_size > 0)) {
        blockid_t current_block_number = current_inode->blocks[current_block_cursor++];
        bm->read_block(current_block_number, tmp_block_data);
        if (remaining_size <= BLOCK_SIZE) {
            memcpy(result_string + done_size, tmp_block_data, remaining_size);
            done_size += remaining_size;
            remaining_size = 0;
        }
        else {
            memcpy(result_string + done_size, tmp_block_data, BLOCK_SIZE);
            done_size += BLOCK_SIZE;
            remaining_size -= BLOCK_SIZE;
        }
    }
    if (remaining_size == 0) {
        free(current_inode);
        return;
    }
    else if (current_block_cursor == NDIRECT) {

        int indirect_block_cursor = 0;
        char indirect_block_data[BLOCK_SIZE];
        bm->read_block(current_inode->blocks[NDIRECT], indirect_block_data);
        while(remaining_size > 0) {
            printf("done_size = %d, remaining_size = %d\n", done_size, remaining_size);
            char current_direct_block_number_string[IndexLengthInIndirectBlock];
            memcpy(current_direct_block_number_string, indirect_block_data + indirect_block_cursor * IndexLengthInIndirectBlock, IndexLengthInIndirectBlock);
            blockid_t current_direct_block_number = atoi(current_direct_block_number_string);
            printf("%s current_direct_block_number = %d \n",current_direct_block_number_string, current_direct_block_number);
            bm->read_block(current_direct_block_number, tmp_block_data);
            if (remaining_size <= BLOCK_SIZE) {
                memcpy(result_string + done_size, tmp_block_data, remaining_size);
                done_size += remaining_size;
                remaining_size = 0;
                free(current_inode);
                return;
            }
            else {
                memcpy(result_string + done_size, tmp_block_data, BLOCK_SIZE);
//                printf("calculated length = %d\n", strlen(result_string));
                done_size += BLOCK_SIZE;
                remaining_size -= BLOCK_SIZE;
            }
            indirect_block_cursor++;
        }
    }
    assert(false);

  
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode* last_inode = get_inode(inum);
  short last_type = 1;
  if (last_inode != NULL) {
      last_type = last_inode->type;
  }


  free_inode(inum);
  verbose = true;
//  if(verbose) printf("开始写操作 inode- %d\n", inum);
  inode* current_inode = get_inode(inum);
  if (current_inode == NULL) {
//      printf("definately null \n");
      current_inode = (struct inode*)malloc(sizeof(struct inode));
      current_inode->type = last_type;
  }
  current_inode->size = (unsigned int)size;
  int remaining_size = size;
  int done_size = 0;
  int current_block_cursor = 0;
  char substring_buf[BLOCK_SIZE];
//  if(verbose) printf("\t size = %d buf = %s \n", size, buf);
//  if(verbose) printf("\t remaining_size = %d", remaining_size);
  while ((current_block_cursor < NDIRECT) && (remaining_size > 0)) {
      //if(verbose) printf("\t 当前写到了第%d个block, remaining_size = %d \n", current_block_cursor, remaining_size);
      blockid_t allocated_direct_block_number = bm->alloc_block();
      if (remaining_size > BLOCK_SIZE) {
          memcpy(substring_buf, buf + done_size, BLOCK_SIZE);

          done_size += BLOCK_SIZE;
          remaining_size -= BLOCK_SIZE;
      }
      else {
          memcpy(substring_buf, buf + done_size, remaining_size);
          done_size += remaining_size;
          remaining_size = 0;
      }
//      if(verbose) printf("\t writing = %s \n", substring_buf);
      bm->write_block(allocated_direct_block_number, substring_buf);
      current_inode->blocks[current_block_cursor++] = allocated_direct_block_number;
//      if(verbose) printf("\t current_inode->blocks[%d] = %d \n", current_block_cursor - 1, allocated_direct_block_number);
  }

  if (remaining_size == 0) {
//      if (verbose) printf("\t quick end in write \n");
      put_inode(inum, current_inode);
      free(current_inode);

      inode* reloaded_inode = get_inode(inum);
//      if (verbose) printf("\t reloaded inode.size = %d \n", reloaded_inode->size);
      free(reloaded_inode);
      return;
  }
  else if (current_block_cursor == NDIRECT){ //current_block_cursor = NDIRECT
      blockid_t allocated_indirect_block_number = bm->alloc_block();
//      if (verbose) printf("\t create indirect number = %d \n", allocated_indirect_block_number);
      char indirect_block_data[BLOCK_SIZE];
      int current_indirect_block_cursor = 0;
      current_inode->blocks[current_block_cursor] = allocated_indirect_block_number;
      while (remaining_size > 0) {
          //if(verbose) printf("\t 当前写到了第%d个block, remaining_size = %d \n", current_indirect_block_cursor, remaining_size);
          blockid_t allocated_direct_block_number = bm->alloc_block();
          if (remaining_size > BLOCK_SIZE) {
              memcpy(substring_buf, buf + done_size, BLOCK_SIZE);
              done_size += BLOCK_SIZE;
              remaining_size -= BLOCK_SIZE;
          }
          else {
              memcpy(substring_buf, buf + done_size, remaining_size);
              done_size += remaining_size;
              remaining_size = 0;
          }
          bm->write_block(allocated_direct_block_number, substring_buf);
          //todo : we need to write the allocated_direct_block_number into the indirect_block_data
          char current_direct_block_number_string[IndexLengthInIndirectBlock];
          sprintf(current_direct_block_number_string,"%d",allocated_direct_block_number);
          assert((current_indirect_block_cursor * IndexLengthInIndirectBlock + IndexLengthInIndirectBlock) < BLOCK_SIZE);
          memcpy(indirect_block_data + current_indirect_block_cursor * IndexLengthInIndirectBlock, current_direct_block_number_string, IndexLengthInIndirectBlock);

          char current_direct_block_number_string_[IndexLengthInIndirectBlock];
          memcpy(current_direct_block_number_string_, indirect_block_data + current_indirect_block_cursor * IndexLengthInIndirectBlock, IndexLengthInIndirectBlock);
          blockid_t current_direct_block_number = atoi(current_direct_block_number_string_);
//          if(verbose) printf("allocated blockid = %d,  read blockid = %d", allocated_direct_block_number, current_direct_block_number);

          current_indirect_block_cursor ++;
      }
      char last_end_marker[IndexLengthInIndirectBlock];
      sprintf(last_end_marker,"%d",-1);
      memcpy(indirect_block_data + current_indirect_block_cursor * IndexLengthInIndirectBlock, last_end_marker, IndexLengthInIndirectBlock);
      bm->write_block(allocated_indirect_block_number, indirect_block_data);
      put_inode(inum, current_inode);
      free(current_inode);
      inode* reloaded_inode = get_inode(inum);
//      if (verbose) printf("\t reloaded inode.size = %d \n", reloaded_inode->size);
      free(reloaded_inode);
      return;
  }
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
//    printf("enter getattr \n");
    inode* tmp_inode = get_inode(inum);
    if (tmp_inode == NULL) {
        return;
    }
    a.size = tmp_inode->size;
    a.atime = tmp_inode->atime;
    a.mtime = tmp_inode->mtime;
    a.ctime = tmp_inode->ctime;
    a.type = tmp_inode->type;
    //printf("trying to get attr with node = %d, type = %d, atime = %d, mtime = %d, ctime = %d\n", inum, a.type, a.atime, a.mtime, a.ctime);
    free(tmp_inode);
    return;
}

void inode_manager::setattr(uint32_t inum, short type, unsigned int atime, unsigned int mtime, unsigned int ctime)
{
    inode* tmp_inode = get_inode(inum);
    if (tmp_inode == NULL) {
        return;
    }
    printf("trying to set attr with node = %d, type = %d, atime = %d, mtime = %d, ctime = %d\n", inum, type, atime, mtime, ctime);
    if (atime > 0)
        tmp_inode->atime = atime;
    if (mtime > 0)
        tmp_inode->mtime = mtime;
    if (ctime > 0)
        tmp_inode->ctime = ctime;
    if (type > 0)
        tmp_inode->type = type;
    put_inode(inum, tmp_inode);
    free(tmp_inode);
    return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
//  printf("enter removal\n");
  verbose = true;
  inode* current_inode = get_inode(inum);
  if (current_inode == NULL) {
      return;
  }
  free_inode(inum);
  free(current_inode);
//  printf("return from removal \n");
  return;
}

