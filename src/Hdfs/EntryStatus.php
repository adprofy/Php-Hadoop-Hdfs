<?php
namespace Hdfs;


/**
* Result of `stat` or `readDir` command for a single filesystem entry
*
*/
class EntryStatus
{
    /** @var string Entry name */
    protected $name;
    /** @var string Entry type (file, directory, symlink). One of `hdfs\EntryType` constants */
    protected $type;
    /** @var int Permissions mask */
    protected $mode;
    /** @var double Entry size in bytes */
    protected $size;
    /** @var int Modification time (unix timestamp) */
    protected $mtime;
    /** @var string Entry owner (user name) */
    protected $owner;
    /** @var string Owner's group */
    protected $group;
    /** @var int Replication factor */
    protected $rfactor;


    /**
    * Constructor
    *
    * @param array $node - parsed result of `stat` or `readDir` command
    */
    public function __construct (array $node)
    {
        $this->name    = $node['name'];
        $this->type    = $node['type'];
        $this->mode    = $node['mode'];
        $this->size    = $node['size'];
        $this->mtime   = $node['mtime'];
        $this->owner   = $node['owner'];
        $this->group   = $node['group'];
        $this->rfactor = $node['rfactor'];
    }//function __construct()


    /**
    * Get entry name
    *
    * @return string
    */
    public function getName ()
    {
        return $this->name;
    }//function getName()


    /**
    * Get entry type.
    * One of `hdfs\EntryType` constants
    *
    * @return string
    */
    public function getType ()
    {
        return $this->type;
    }//function getType()


    /**
    * Returns 'true' if this entry represents file
    *
    * @return bool
    */
    public function isFile ()
    {
        return $this->type == EntryType::FILE;
    }//function isFile()


    /**
    * Returns 'true' if this entry represents directory
    *
    * @return bool
    */
    public function isDir ()
    {
        return $this->type == EntryType::DIRECTORY;
    }//function isDir()

    /**
    * Get permissions
    *
    * @return int
    */
    public function getMode ()
    {
        return $this->mode;
    }//function getMode()


    /**
    * Get octal representation of permissions mode.
    * Returns string containing octal number without leading zero.
    *
    * @return string
    */
    public function getOctMode ()
    {
        return decoct($this->getMode());
    }//function getOctMode()


    /**
    * Get Entry size in bytes
    *
    * @return double
    */
    public function getSize ()
    {
        return $this->size;
    }//function getSize()


    /**
    * Get modification time.
    * Unix timestamp.
    *
    * @return int
    */
    public function getMtime ()
    {
        return $this->mtime;
    }//function getMtime()


    /**
    * Get entry owner (user's name)
    *
    * @return string
    */
    public function getOwner ()
    {
        return $this->owner;
    }//function getOwner()


    /**
    * Get entry owner's group
    *
    * @return string
    */
    public function getGroup ()
    {
        return $this->group;
    }//function getGroup()


    /**
    * Get entry replication factor.
    * Returns 0 if this entry is directory.
    *
    * @return int
    */
    public function getRfactor ()
    {
        return $this->rfactor;
    }//function getRfactor()


    /**
    * Get associative array which represents this entry.
    * Array fields: name, type, size, mode, group, owner, mtime, rfactor
    *
    * @return array
    */
    public function toArray ()
    {
        $node = array(
            'name'    => $this->name,
            'type'    => $this->type,
            'mode'    => $this->mode,
            'size'    => $this->size,
            'mtime'   => $this->mtime,
            'owner'   => $this->owner,
            'group'   => $this->group,
            'rfactor' => $this->rfactor
        );

        return $node;
    }//function toArray()


}//class EntryStatus