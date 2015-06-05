<?php
namespace Hdfs;

use Hdfs\Web\Method;


/**
* HDFS client implementation using WebHDFS
*
*/
class Web extends HdfsAbstract
{
    /** @var \Hdfs\Web\WebHdfsWrapper WebHDFS connection */
    protected $web = null;


    /**
    * Constructor
    *
    * @param null|\Hdfs\Web\WebHdfsWrapper $webHdfs Wrapper for `hadoop fs` command
    * @param null|\Hdfs\FilesystemWrapper $localfs Wrapper for standart php filesystem functions
    */
    public function __construct (Web\WebHdfsWrapper $webHdfs = null, FilesystemWrapper $localfs = null)
    {
        $this->localfs = ($localfs ?: new FilesystemWrapper());
        $this->web     = ($webHdfs ?: new Web\WebHdfsWrapper());
    }//function __construct()


    /**
    * Change wrapper for WebHDFS
    *
    * @param \Hdfs\Web\WebHdfsWrapper $localfs
    */
    public function setWebHdfsWrapper (Web\WebHdfsWrapper $web)
    {
        $this->web = $web;
    }//function setWebHdfsWrapper()


    /**
    * Set callback wich will be invoked on each request to WebHDFS.
    * Callback should accept single argument of type \Hdfs\Web\Response.
    *
    * @param \Closure $callback
    */
    public function setDebugCallback (\Closure $callback)
    {
        $this->web->setDebugCallback($callback);
    }//function setDebugCallback()


    /**
    * Set connection parameters
    *
    * @param string $host
    * @param int $port
    * @param null|string $user If `null` given, current php process owner will be used
    */
    public function configure ($host, $port, $user = null)
    {
        $this->web->configure($host, $port, $user);
    }//function configure()


    /**
    * Get connection parameters
    *
    * @return array Associative array with keys: host, port, user
    */
    public function getConfig ()
    {
        return $this->web->getConfig();
    }//function getConfig()


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

        //do not upload file if selected directory does not exist
        try
        {
            $this->stat(dirname($hdfsFile));
        }
        catch (Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        $uploadUrl = $this->web->getDatanodeUrl(Method::PUT, $hdfsFile, 'CREATE', array('overwrite' => 'false'));
        $response  = $this->web->put($uploadUrl, $localFile);
        if ($response->getException())
        {
            throw $response->getException();
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

        $response = $this->web->exec(Method::GET, $hdfsFile, 'OPEN');
        if ($response->getException())
        {
            throw $response->getException();
        }

        $this->localfs->saveFile($localFile, $response->getFileContent());
    }//function getFile()


    /**
    * Remove file from HDFS
    *
    * @param string $hdfsFile
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  If $hdfsFile is not a file
    * @throws \Hdfs\Exception\NotFoundException         If $hdfsFile does not exist
    * @throws \Hdfs\Exception\PermissionException       If unable to access $hdfsFile due to permission restrictions
    */
    public function removeFile ($hdfsFile)
    {
        //throw exception if file does not exist
        try
        {
            $entry = $this->stat($hdfsFile);

            if (!$entry->isFile())
            {
                throw new Exception\IllegalArgumentException("Provided path is not a file: $hdfsFile", false);
            }
        }
        catch (Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        $response = $this->web->exec(Method::DELETE, $hdfsFile, 'DELETE');
        if ($response->getException())
        {
            throw $response->getException();
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

        //throw exception if file does not exist or $hdfsFile is directory
        try
        {
            $entry = $this->stat($hdfsFile);
            if (!$entry->isFile())
            {
                throw new Exception\IllegalArgumentException("Provided path is not a file: $hdfsFile", false);
            }
        }
        catch (Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        $response = $this->web->exec(Method::PUT, $hdfsFile, 'SETREPLICATION', array('replication' => $factor));
        if ($response->getException())
        {
            throw $response->getException();
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
        try
        {
            $this->stat($hdfsDir);
            //directory already exists
            throw new Exception\AlreadyExistsException("Path already exists: $hdfsDir", false);
        }
        catch(Exception\NotFoundException $e)
        {
            //dir does not exist so we can create it
        }

        //if parent directory does not exist
        try
        {
            $this->stat(dirname($hdfsDir));
        }
        catch (Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        $response = $this->web->exec(Method::PUT, $hdfsDir, 'MKDIRS');
        if ($response->getException())
        {
            throw $response->getException();
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
        try
        {
            $entry = $this->stat($hdfsDir);
            if (!$entry->isDir())
            {
                throw new Exception\IllegalArgumentException("Provided path is not a directory: $hdfsDir", true);
            }
        }
        catch(Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException("Path does not exist: $hdfsDir", false);
        }

        $response = $this->web->exec(Method::DELETE, $hdfsDir, 'DELETE');
        if ($response->getException())
        {
            throw $response->getException();
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
        try
        {
            $entry = $this->stat($hdfsDir);
            if (!$entry->isDir())
            {
                throw new Exception\IllegalArgumentException("Provided path is not a directory: $hdfsDir", false);
            }
        } //rethrow exception, so it point to correct method
        catch(Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        $response = $this->web->exec(Method::GET, $hdfsDir, 'LISTSTATUS');
        if ($response->getException())
        {
            throw $response->getException();
        }

        $list = array();
        $statuses = $response->getJson()->FileStatuses->FileStatus;
        foreach ($statuses as $stat)
        {
            $entry = array(
                'name'    => $stat->pathSuffix,
                'type'    => $stat->type,
                'mode'    => octdec($stat->permission),
                'size'    => $stat->length,
                'mtime'   => $stat->modificationTime,
                'owner'   => $stat->owner,
                'group'   => $stat->group,
                'rfactor' => $stat->replication,
            );

            $list[] = ($isAssoc ? $entry : new EntryStatus($entry));
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
        $response = $this->web->exec(Method::GET, $hdfsPath, 'GETFILESTATUS');

        if ($response->getException())
        {
            throw $response->getException();
        }

        $stat  = $response->getJson()->FileStatus;
        $entry = array(
            'name'    => basename($hdfsPath),
            'type'    => $stat->type,
            'mode'    => octdec($stat->permission),
            'size'    => $stat->length,
            'mtime'   => $stat->modificationTime,
            'owner'   => $stat->owner,
            'group'   => $stat->group,
            'rfactor' => $stat->replication,
        );

        if (!$isAssoc)
        {
            $entry = new EntryStatus($entry);
        }

        return $entry;
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

        $response = $this->web->exec(Method::PUT, $hdfsPath, 'SETPERMISSION', array('permission' => decoct($mode)));
        if ($response->getException())
        {
            throw $this->getException();
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

        $args = array();
        if (trim($owner))
        {
            $args['owner'] = $owner;
        }
        if (trim($group))
        {
            $args['group'] = $group;
        }

        $response = $this->web->exec(Method::PUT, $hdfsPath, 'SETOWNER', $args);
        if ($response->getException())
        {
            throw $response->getException();
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
        try
        {
            $this->stat($hdfsSrcPath);
        } //rethrow exception, so it point to correct method
        catch(Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        //check if destination directory does not exist
        try
        {
            $this->stat(dirname($hdfsDstPath));
        } //rethrow exception, so it point to correct method
        catch(Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        //check if destination path already exists
        try
        {
            $this->stat($hdfsDstPath);
            throw new Exception\AlreadyExistsException("Path already exists: $hdfsDstPath", 2);
        }
        catch(Exception\NotFoundException $e)
        {
        }

        $response = $this->web->exec(Method::PUT, $hdfsSrcPath, 'RENAME', array('destination' => $hdfsDstPath));
        if ($response->getException())
        {
            throw $response->getException();
        }
    }//function rename()


    /**
    * Get file content.
    *
    * @param string $hdfsFile
    * @param int $offset Get content starting from this position in file.
    * @param int $length Read this amount of bytes. If $length = 0, read till the end of file.
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  If $hdfsFile is not a file
    * @throws \Hdfs\Exception\NotFoundException If file does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to read file due to permissions restrictions.
    *
    * @return string
    */
    public function readFile ($hdfsFile, $offset = 0, $length = 0)
    {
        $entry = $this->stat($hdfsFile);
        if (!$entry->isFile())
        {
            throw new Exception\IllegalArgumentException("Provided path is not a file: $hdfsFile", false);
        }

        $args = array();
        if ($offset)
        {
            $args['offset'] = $offset;
        }
        if ($length)
        {
            $args['length'] = $length;
        }

        $response = $this->web->exec(Method::GET, $hdfsFile, 'OPEN', $args);
        if ($response->getException())
        {
            throw $response->getException();
        }

        return $response->getFileContent();
    }//function readFile()


    /**
    * Write content to file.
    *
    * @param string $hdfsFile
    * @param string $content
    *
    * @throws \Hdfs\Exception\AlreadyExistsException If $hdfsFile already exists.
    * @throws \Hdfs\Exception\NotFoundException If directory for file does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to write to file due to permissions restrictions.
    *
    * @return string
    */
    public function writeFile ($hdfsFile, $content)
    {
        //do not create file if selected directory does not exist
        try
        {
            $this->stat(dirname($hdfsFile));
        }
        catch (Exception\NotFoundException $e)
        {
            throw new Exception\NotFoundException($e->getMessage(), $e->isLocal());
        }

        $size = strlen($content);

        $f = $this->localfs->openFile("php://temp/maxmemory:$size", 'w');
        $this->localfs->write($f, $content);
        $this->localfs->seek($f, 0);

        $uploadUrl = $this->web->getDatanodeUrl(Method::PUT, $hdfsFile, 'CREATE', array('overwrite' => 'false'));
        $response  = $this->web->put($uploadUrl, $f, array(), $size);
        if ($response->getException())
        {
            throw $response->getException();
        }

        $this->localfs->closeFile($f);
    }//function writeFile()


}//class Web