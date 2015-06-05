<?php
namespace Hdfs;


/**
* Possible types of HDFS filesystem entries
*
*/
class EntryType
{

    /** Entry type for files */
    const FILE = 'FILE';
    /** Entry type for direcotries */
    const DIRECTORY = 'DIRECTORY';
    /**
    * Entry type for symlinks
    * :INFO:
    * Symlinks are not supported by Hadoop at this moment
    */
    const SYMLINK = 'SYMLINK';

}//class EntryType