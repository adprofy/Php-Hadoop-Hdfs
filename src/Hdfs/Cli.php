<?php
namespace Hdfs;



/**
* HDFS client implementation using CLI tool `hadoop fs`
*
*/
class Cli extends HdfsAbstract
{
    /** @var \Hdfs\Cli\HadoopWrapper `hadoop fs` wrapper */
    protected $cli = null;


    /**
    * Constructor
    *
    * @param null|\Hdfs\FilesystemWrapper $localfs Wrapper for standart php filesystem functions
    * @param null|\Hdfs\Cli\HadoopWrapper $hadoopCli Wrapper for `hadoop fs` command
    */
    public function __construct (Cli\HadoopWrapper $hadoopCli = null, FilesystemWrapper $localfs = null)
    {
        $this->localfs = ($localfs ?: new FilesystemWrapper());
        $this->cli     = ($hadoopCli ?: new Cli\HadoopWrapper());
    }//function __construct()


    /**
    * Change wrapper for 'hadoop fs' command
    *
    * @param \Hdfs\Cli\HadoopWrapper $localfs
    */
    public function setHadoopWrapper (Cli\HadoopWrapper $cli)
    {
        $this->cli = $cli;
    }//function setHadoopWrapper()


    /**
    * Set callback wich will be invoked on each request to CLI `hadoop fs` command.
    * Callback should accept single argument of type \Hdfs\Cli\Response.
    *
    * @param \Closure $callback
    */
    public function setDebugCallback (\Closure $callback)
    {
        $this->cli->setDebugCallback($callback);
    }//function setDebugCallback()


    /**
    * Upload $localFile to Hadoop at $hdfsFile path
    *
    * @param string $localFile
    * @param string $hdfsFile
    *
    * @throws \Hdfs\Exception\NotFoundException         if either $localFile or directory for $hdfsFile not exist
    * @throws \Hdfs\Exception\AlreadyExistsException    if $hdfsFile already exists
    * @throws \Hdfs\Exception\PermissionException       if unable to access $localFile or $hdfsFile due to permission restrictions
    */
    public function putFile ($localFile, $hdfsFile)
    {
        if (!$this->localfs->isExists($localFile))
        {
            throw new Exception\NotFoundException("Local file does not exist: $localFile", true);
        }
        if (!$this->localfs->isReadable($localFile))
        {
            throw new Exception\PermissionException("Unable to read local file: $localFile", true);
        }

        $result = $this->cli->exec('-put', $localFile, $hdfsFile);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function putFile()


    /**
    * Download $hdfsFile from HDFS to $localFile in local filesystem
    *
    * @param string $hdfsFile
    * @param string $localFile
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  if $hdfsFile is not a file
    * @throws \Hdfs\Exception\NotFoundException         if either $hdfsFile or directory for $localFile not exist
    * @throws \Hdfs\Exception\AlreadyExistsException    if $localFile already exists
    * @throws \Hdfs\Exception\PermissionException       if unable to access $localFile or $hdfsFile due to permission restrictions
    */
    public function getFile ($hdfsFile, $localFile)
    {
        $dir = dirname($localFile);
        if (!$this->localfs->isExists($dir))
        {
            throw new Exception\NotFoundException("No such local file or directory: $dir", true);
        }
        if (!$this->localfs->isWritable($dir))
        {
            throw new Exception\PermissionException("Unable to write to local directory: $dir", true);
        }

        if ($this->localfs->isExists($localFile))
        {
            throw new Exception\AlreadyExistsException("Local file already exists: $localFile", true);
        }

        $entry = $this->stat($hdfsFile);
        if (!$entry->isFile())
        {
            throw new Exception\IllegalArgumentException("Provided path is not a file: $hdfsFile", false);
        }

        $result = $this->cli->exec('-get', $hdfsFile, $localFile);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function getFile()


    /**
    * Remove file from HDFS
    *
    * @param string $hdfsFile
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  if $hdfsFile is not a file
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsFile does not exist
    * @throws \Hdfs\Exception\PermissionException       if unable to access $hdfsFile due to permission restrictions
    */
    public function removeFile ($hdfsFile)
    {
        $result = $this->cli->exec('-rm', '-skipTrash', $hdfsFile);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function removeFile()


    /**
    * Set replication factor of a file $hdfsFile
    *
    * @param string $hdfsFile
    * @param int $factor        replication factor
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  if $hdfsFile is a directory or $factor is not a positive integer
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsFile does not exist
    */
    public function setFileReplication ($hdfsFile, $factor)
    {
        if ($factor < 0 || !is_numeric($factor) || $factor != intval($factor))
        {
            throw new Exception\IllegalArgumentException("Positive integer expected for \$factor argument, but '$factor' given.", false);
        }

        $result = $this->cli->exec('-test', '-d', $hdfsFile);

        if ($result->getException())
        {
            throw $result->getException();
        }
        if ($result->exitCode === 0)
        {
            throw new Exception\IllegalArgumentException("File path expected, but directory path given: $hdfsFile", false);
        }

        $result = $this->cli->exec('-setrep', $factor, $hdfsFile);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function setFileReplication()


    /**
    * Create $hdfsDir directory.
    *
    * @param string $hdfsDir
    *
    * @throws \Hdfs\Exception\NotFoundException         if parent directory does not exist
    * @throws \Hdfs\Exception\PermissionException       if unable to create directory due to permission restrictions
    * @throws \Hdfs\Exception\AlreadyExistsException    if $hdfsDir already exists
    * @throws \Hdfs\Exception\IllegalArgumetnException  if parent directory of $hdfsDir is not a directory
    */
    public function makeDir ($hdfsDir)
    {
        $result = $this->cli->exec('-mkdir', $hdfsDir);

        if ($result->getException())
        {
            $exception = $result->getException();
            //IllegalArgument on $hdfsDir means `is not a directory`, change it to AlreadyExists
            if ($exception instanceof Exception\IllegalArgumentException && stripos($exception->getMessage(), $hdfsDir) !== false)
            {
                $exception = new Exception\AlreadyExistsException($exception->getMessage(), $exception->isLocal());
            }

            throw $exception;
        }
    }//function makeDir()


    /**
    * Remove $hdfsDir directory.
    * Directory should be empty.
    *
    * @param string $hdfsDir
    *
    * @throws \Hdfs\Exception\NotFoundException         if directory does not exist
    * @throws \Hdfs\Exception\NotEmptyException         if directory is not empty
    * @throws \Hdfs\Exception\PermissionException       if unable to delete directory due to permission restrictions
    * @throws \Hdfs\Exception\IllegalArgumentException  if $hdfsFile is not a directory
    */
    public function removeDir ($hdfsDir)
    {
        $result = $this->cli->exec('-rmdir', $hdfsDir);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function removeDir()


    /**
    * List contents of $hdfsDir directory.
    *
    * Returns array of hdfs\EntryStatus instances
    * or array of arrays with fields: name, type, size, mode, group, owner, mtime, rfactor
    *
    * @param string $hdfsDir
    * @param bool $isAssoc      'false' - return list of hdfs\EntryStatus instances,
    *                           'true' - return list of associative arrays
    *
    * @throws \Hdfs\Exception\NotFoundException         if directory does not exist
    * @throws \Hdfs\Exception\PermissionException       if unable to read directory due to permission restrictions
    * @throws \Hdfs\Exception\IllegalArgumentException  if $hdfsDir is not a directory
    *
    * @return array
    */
    public function readDir ($hdfsDir, $isAssoc = false)
    {
        $result = $this->cli->exec('-ls', $hdfsDir);
        if ($result->getException())
        {
            throw $result->getException();
        }

        $list = Cli\HadoopWrapper::parseLsOutput($result->stdout, $isAssoc, $hdfsDir);

        //check if $hdfsDir is actually a file
        if (count($list) == 1)
        {
            if (($isAssoc && !$list[0]['name']) || !$list[0]->getName())
            {
                throw new Exception\IllegalArgumentException("Directory path expected, but file path given: $hdfsDir", true);
            }
        }

        return $list;
    }//function readDir()


    /**
    * Get stats for $hdfsPath
    *
    * @param string $hdfsPath
    * @param bool $isAssoc      'false' - return hdfs\EntryStatus instance,
    *                           'true' - return associative array
    *
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsPath does not exist
    * @throws \Hdfs\Exception\PermissionException       if $hdfsPath is inaccessible due to permission restrictions
    *
    * @return array|\Hdfs\EntryStatus
    */
    public function stat ($hdfsPath, $isAssoc = false)
    {
        $result = $this->cli->exec('-ls', '-d', $hdfsPath);
        if ($result->getException())
        {
            throw $result->getException();
        }

        $list = Cli\HadoopWrapper::parseLsOutput($result->stdout, $isAssoc, dirname($hdfsPath));

        return $list[0];
    }//function stat()


    /**
    * Change permissions for $hdfsPath
    *
    * @param string $hdfsPath
    * @param int $mode
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  if $mode is not an integer value
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsPath does not exist
    * @throws \Hdfs\Exception\PermissionException       if current user is not allowed to change mode for $hdfsPath
    */
    public function changeMode ($hdfsPath, $mode)
    {
        if (!is_int($mode))
        {
            throw new Exception\IllegalArgumentException("Integer expected for \$mode argument. Given: $mode", false);
        }

        $result = $this->cli->exec('-chmod', decoct($mode), $hdfsPath);
        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function changeMode()


    /**
    * Change owner and/or group for $hdfsPath.
    * At least two arguments should be passed to this method.
    * Either $owner or $group should be set.
    *
    * @param string $hdfsPath
    * @param null|string $owner
    * @param null|string $group
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  if both $owner and $group are not set
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsPath does not exist
    * @throws \Hdfs\Exception\PermissionException       if current user is not allowed to change owner for $hdfsPath
    */
    public function changeOwner ($hdfsPath, $owner = null, $group = null)
    {
        if (!trim($owner) && !trim($group))
        {
            throw new Exception\IllegalArgumentException("Either owner or group should be specified", false);
        }

        $result = $this->cli->exec('-chown', $owner.($group ? ":$group" : ""), $hdfsPath);
        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function changeOwner()


    /**
    * Rename $hdfsSrcPath to $hdfsDstPath
    *
    * @param string $hdfsSrcPath
    * @param string $hdfsDstPath
    *
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsSrcPath or parent directory of $hdfsDstPath does not exist
    * @throws \Hdfs\Exception\AlreadyExistsException    if $hdfsDstPath already exists
    * @throws \Hdfs\Exception\PermissionException       if current user is not allowed to rename $hdfsSrcPath to $hdfsDstPath
    */
    public function rename ($hdfsSrcPath, $hdfsDstPath)
    {
        $result = $this->cli->exec('-mv', $hdfsSrcPath, $hdfsDstPath);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function rename()


    /**
    * Get all the files in the directories that match the $hdfsPath file pattern and
    * merge and store them to only one file on local filesystem - $localFile.
    *
    * @param string $hdfsPath
    * @param string $localFile
    * @param bool $removeCrc Whether to delete automatically created crc file or not.
    * @param bool $addNewLines Add new line character to the end of each file.
    *
    * @throws \Hdfs\Exception\NotFoundException If $hdfsPath or parent directory for $localFile does not exist.
    * @throws \Hdfs\Exception\PermissionException If operation cannot be performed due to permissions restriction.
    * @throws \Hdfs\Exception\AlreadyExistsException If $localFile already exists.
    */
    public function getMerge ($hdfsPath, $localFile, $removeCrc = true, $addNewLines = false)
    {
        if ($this->localfs->isExists($localFile))
        {
            throw new Exception\AlreadyExistsException("Destination path already exists: $localFile", true);
        }
        $localDir = dirname($localFile);
        if (!$this->localfs->isExists($localDir))
        {
            throw new Exception\NotFoundException("Path does not exist: $localDir", true);
        }
        if (!$this->localfs->isWritable($localDir))
        {
            throw new Exception\PermissionException("Unable to write to directory: $localDir", true);
        }

        $result = (
            $addNewLines
                ? $this->cli->exec('-getmerge', '-nl', $hdfsPath, $localFile)
                : $this->cli->exec('-getmerge', $hdfsPath, $localFile)
        );
        if ($result->getException())
        {
            throw $result->getException();
        }

        if ($removeCrc)
        {
            $crcFile = dirname($localFile) .'.'. basename($localFile) .'.crc';
            $this->localfs->removeFile($crcFile);
        }
    }//function getMerge()


    /**
    * Check if $path exists
    *
    * @param string $path
    *
    * @throws \Hdfs\Exception\PermissionException   if unable to access $path due to permission restrictions
    *
    * @return bool
    */
    public function isExists ($path)
    {
        $result = $this->cli->exec('-test', '-e', $path);

        if ($result->getException())
        {
            throw $result->getException();
        }

        return !$result->exitCode;
    }//function isExists()


    /**
    * Create directories recursively.
    *
    * If directory already exists and $mode/$owner/$group provided, then method
    * just tries to apply those arguments to the last component in $hdfsDir.
    *
    * @param string $hdfsDir
    * @param int|null $mode If specified set these permissions to all created directories.
    * @param string|null $owner If specified set this owner to all created directories.
    * @param string|null $group If specified set this user group to all created directories.
    *
    * @throws \Hdfs\Exception\PermissionException If unable to create directory due to permissions restriction.
    * @throws \Hdfs\Exception\IllegalArgumentException If one of components in $hdfsDir path is not a directory or if $mode is neither null nor integer.
    */
    public function makeDirRecursive ($hdfsDir, $mode = null, $owner = null, $group = null)
    {
        $creationQueue = array();

        //find nearest existing directory and gather list of directories to create
        $parent = $hdfsDir;
        while ($parent && $parent != '/')
        {
            try
            {
                $entry = $this->stat($parent);
                if (!$entry->isDir())
                {
                    throw new Exception\IllegalArgumentException("Provided path is not a directory: $parent", false);
                }
                break;
            }
            catch(Exception\NotFoundException $e)
            {
                $creationQueue[] = basename($parent);
                $parent = dirname($parent);
            }
        }

        //dir already exists
        if (!$creationQueue)
        {
            $chmodDir = $hdfsDir;
        }
        else
        {
            //find topmost absent directory
            $topCreatedDir = array_pop($creationQueue);
            $chmodDir = preg_replace('#'. preg_quote($topCreatedDir) .'/(.*)#', $topCreatedDir, $hdfsDir);

            //create directories
            $result = $this->cli->exec('-mkdir', '-p', $hdfsDir);

            if ($result->getException())
            {
                throw $result->getException();
            }
        }

        //changhe mode
        if (!is_null($mode))
        {
            $this->changeModeRecursive($chmodDir, $mode);
        }
        //change owner
        if (trim($owner) || trim($group))
        {
            $this->changeOwnerRecursive($chmodDir, $owner, $group);
        }
    }//function makeDirRecursive()


    /**
    * Change owner/group of $hdfsPath and all files&directories inside of $hdfsPath.
    *
    * @param string $hdfsPath
    * @param string|null $owner
    * @param string|null $group
    *
    * @throws \Hdfs\Exception\IllegalArgumentException If both $owner and $group are not set.
    * @throws \Hdfs\Exception\NotFoundException If $hdfsPath does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to change owner due to permission restrictions.
    */
    public function changeOwnerRecursive ($hdfsPath, $owner = null, $group = null)
    {
        if (!trim($owner) && !trim($group))
        {
            throw new Exception\IllegalArgumentException("Either owner or group should be specified", false);
        }

        $result = $this->cli->exec('-chown', '-R', $owner.($group ? ":$group" : ""), $hdfsPath);
        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function changeOwnerRecursive()


    /**
    * Change permissions of $hdfsPath and all files&directories inside of $hdfsPath.
    *
    * @param string $hdfsPath
    * @param int $mode
    *
    * @throws \Hdfs\Exception\IllegalArgumentException If $mode is not an integer.
    * @throws \Hdfs\Exception\NotFoundException If $hdfsPath does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to change mode due to permission restrictions.
    */
    public function changeModeRecursive ($hdfsPath, $mode)
    {
        if (!is_int($mode))
        {
            throw new Exception\IllegalArgumentException("Integer expected for \$mode argument. Given: $mode", false);
        }

        $result = $this->cli->exec('-chmod', '-R', decoct($mode), $hdfsPath);
        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function changeModeRecursive()


    /**
    * Delete file or directory recursively.
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\NotFoundException If path does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to delete due to permissions restrictions.
    */
    public function deleteRecursive ($hdfsPath)
    {
        $result = $this->cli->exec('-rm', '-r', '-skipTrash', $hdfsPath);

        if ($result->getException())
        {
            throw $result->getException();
        }
    }//function deleteRecursive()


    /**
    * Get filesystem object size in bytes.
    * Recursively gathers size information for directories.
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\NotFoundException If path does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to stat specified path due to permissions restrictions.
    *
    * @return int
    */
    public function getSizeRecursive ($hdfsPath)
    {
        $result = $this->cli->exec('-count', $hdfsPath);
        if ($result->getException())
        {
            throw $result->getException();
        }

        $parts = preg_split('/\s+/', $result->stdout);
        $size = (int)$parts[3];

        return $size;
    }//function getSizeRecursive()

}//class Cli