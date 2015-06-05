<?php
namespace Hdfs;


/**
* Abstract class for Hdfs utility implementations
*
*/
abstract class HdfsAbstract
{
    /** @var \Hdfs\FilesystemWrapper Wrapper for standart php filesystem functions */
    protected $localfs = null;


    /**
    * Change wrapper for local filesystem
    *
    * @param \Hdfs\FilesystemWrapper $localfs
    */
    public function setFilesystemWrapper (FilesystemWrapper $localfs)
    {
        $this->localfs = $localfs;
    }//function setFilesystemWrapper()


    /**
    * Set callback wich will be invoked on each request to service provider.
    * Callback should accept single argument of type \Hdfs\IResponse or one of its descendants.
    *
    * @param \Closure $callback
    */
    abstract public function setDebugCallback (\Closure $callback) ;


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
    abstract public function putFile ($localFile, $hdfsFile) ;


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
    abstract public function getFile ($hdfsFile, $localFile) ;


    /**
    * Remove file from HDFS
    *
    * @param string $hdfsFile
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  If $hdfsFile is not a file
    * @throws \Hdfs\Exception\NotFoundException         If $hdfsFile does not exist
    * @throws \Hdfs\Exception\PermissionException       If unable to access $hdfsFile due to permission restrictions
    */
    abstract public function removeFile ($hdfsFile) ;


    /**
    * Set replication factor of a file $hdfsFile
    *
    * @param string $hdfsFile
    * @param int $factor        replication factor
    *
    * @throws \Hdfs\Exception\IllegalArgumentException  if $hdfsFile is a directory or $factor is not a positive integer
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsFile does not exist
    */
    abstract public function setFileReplication ($hdfsFile, $factor) ;


    /**
    * Create $hdfsDir directory.
    *
    * @param string $hdfsDir
    *
    * @throws \Hdfs\Exception\NotFoundException         if parent directory does not exist
    * @throws \Hdfs\Exception\PermissionException       if unable to create directory due to permission restrictions
    * @throws \Hdfs\Exception\AlreadyExistsException    if $hdfsDir already exists
    * @throws \Hdfs\Exception\IllegalArgumentException  if parent directory of $hdfsDir is not a directory
    */
    abstract public function makeDir ($hdfsDir) ;


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
    abstract public function removeDir ($hdfsDir) ;


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
    abstract public function readDir ($hdfsDir, $isAssoc = false) ;


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
    abstract public function stat ($hdfsPath, $isAssoc = false) ;


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
    abstract public function changeMode ($hdfsPath, $mode) ;


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
    abstract public function changeOwner ($hdfsPath, $owner = null, $group = null) ;


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
    abstract public function rename ($hdfsSrcPath, $hdfsDstPath) ;


    /**
    * Check if $hdfsPath exists
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\PermissionException       if $hdfsPath is inaccessible due to permission restrictions
    *
    * @return bool
    */
    public function isExists ($hdfsPath)
    {
        try
        {
            $this->stat($hdfsPath);
        }
        catch (Exception\NotFoundException $e)
        {
            return false;
        }

        return true;
    }//function isExists()


    /**
    * Check if $hdfsPath is directory
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsPath does not exist
    * @throws \Hdfs\Exception\PermissionException       if $hdfsPath is inaccessible due to permission restrictions
    *
    * @return bool
    */
    public function isDir ($hdfsPath)
    {
        return $this->stat($hdfsPath)->isDir();
    }//function isDir()


    /**
    * Check if $hdfsPath is file
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\NotFoundException         if $hdfsPath does not exist
    * @throws \Hdfs\Exception\PermissionException       if $hdfsPath is inaccessible due to permission restrictions
    *
    * @return bool
    */
    public function isFile ($hdfsPath)
    {
        return $this->stat($hdfsPath)->isFile();
    }//function isFile()


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
        //path already exists
        try
        {
            $entry = $this->stat($hdfsDir);
            if (!$entry->isDir())
            {
                throw new Exception\IllegalArgumentException("Provied path already exists and it's not a directory: $hdfsDir", false);
            }
            if (!is_null($mode))
            {
                $this->changeMode($hdfsDir, $mode);
            }
            if (trim($owner) || trim($group))
            {
                $this->changeOwner($hdfsDir, $owner, $group);
            }

            return;
        }
        catch (Exception\NotFoundException $e)
        {
        }

        $creationQueue = array(basename($hdfsDir));

        //find nearest existing directory and gather list of directories to create
        $parent = dirname($hdfsDir);
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

        //create directories
        $path = preg_replace('#/$#', '', $parent);
        while (count($creationQueue) > 0)
        {
            $dir  = array_pop($creationQueue);
            $path .= "/$dir";

            $this->makeDir($path);
            if (!is_null($mode))
            {
                $this->changeMode($path, $mode);
            }
            if (trim($owner) || trim($group))
            {
                $this->changeOwner($path, $owner, $group);
            }
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
            throw new Exception\IllegalArgumentException('Either $owner or $group should be set.', false);
        }

        $entry = $this->stat($hdfsPath);
        $this->changeOwner($hdfsPath, $owner, $group);

        //single file
        if (!$entry->isDir())
        {
            return;
        }

        $path = preg_replace('#/$#', '', $hdfsPath);
        $toChangeList = array(
            $path => $this->readDir($hdfsPath)
        );
        while ($toChangeList)
        {
            $queued = array();

            foreach ($toChangeList as $path => $entries)
            {
                foreach ($entries as $entry)
                {
                    $hdfsName = "$path/{$entry->getName()}";
                    $this->changeOwner($hdfsName, $owner, $group);

                    if ($entry->isDir())
                    {
                        $queued[$hdfsName] = $this->readDir($hdfsName);
                    }
                }
            }

            $toChangeList = $queued;
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
            throw new Exception\IllegalArgumentException('$mode should be an integer.', false);
        }

        $entry = $this->stat($hdfsPath);
        $this->changeMode($hdfsPath, $mode);

        //single file
        if (!$entry->isDir())
        {
            return;
        }

        $path = preg_replace('#/$#', '', $hdfsPath);
        $toChangeList = array(
            $path => $this->readDir($hdfsPath)
        );
        while ($toChangeList)
        {
            $queued = array();

            foreach ($toChangeList as $path => $entries)
            {
                foreach ($entries as $entry)
                {
                    $hdfsName = "$path/{$entry->getName()}";
                    $this->changeMode($hdfsName, $mode);

                    if ($entry->isDir())
                    {
                        $queued[$hdfsName] = $this->readDir($hdfsName);
                    }
                }
            }

            $toChangeList = $queued;
        }
    }//function changeModeRecursive()


    /**
    * Uploads $localDir to $hdfsDir.
    * If $hdfsDir does not exist, creates it. But if parent directory of $hdfsDir does not exist, throws exception.
    *
    * @param string $localDir
    * @param string $hdfsDir
    * @param bool $isOverwrite Overwrite existing files with the same names.
    *
    * @throws \Hdfs\Exception\IllegalArgumentException If $localDir is not a directory.
    * @throws \Hdfs\Exception\NotFoundException If $localDir or parent directory of $hdfsDir do not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to read local path or unable to write to HDFS path.
    * @throws \Hdfs\Exception\AlreadyExistsException If unable to overwrite existing files.
    */
    public function putDir ($localDir, $hdfsDir, $isOverwrite = false)
    {
        if (!$this->localfs->isExists($localDir))
        {
            throw new Exception\NotFoundException("Path not found: $localDir", true);
        }
        if (!$this->localfs->isDir($localDir))
        {
            throw new Exception\IllegalArgumentException("Provided path is not a direcotry: $localDir", true);
        }
        if (!$this->localfs->isReadable($localDir))
        {
            throw new Exception\PermissionException("Provided path is not a direcotry: $localDir", true);
        }
        if (!$this->isExists(dirname($hdfsDir)))
        {
            throw new Exception\NotFoundException("Directory does not exist: ". dirname($hdfsDir), true);
        }
        if (!$this->isExists($hdfsDir))
        {
            $this->makeDir($hdfsDir);
        }

        $localDir = preg_replace('#/$#', '', $localDir);
        $hdfsDir  = preg_replace('#/$#', '', $hdfsDir);

        //collect list of files and directories to upload
        $queue = array();
        $list  = $this->localfs->readDir($localDir);
        while ($list)
        {
            $path = array_shift($list);
            if (!$this->localfs->isReadable($path))
            {
                throw new Exception\PermissionException("Unable to read path: $path", true);
            }
            if ($this->localfs->isDir($path))
            {
                $queue[$path] = true;
                $list = array_merge($list, $this->localfs->readDir($path));
            }
            else
            {
                $queue[$path] = false;
            }
        }

        //create directories & upload files
        foreach ($queue as $localPath => $isDir)
        {
            $path     = str_replace("$localDir/", '', $localPath);
            $hdfsPath = "$hdfsDir/$path";
            $entry    = null;

            try
            {
                $entry = $this->stat($hdfsPath);
                if ($entry->isDir() != $isDir || !$isOverwrite)
                {
                    $hdfsType = ($entry->isDir() ? 'directory' : 'file');
                    $localType = ($isDir ? 'directory' : 'file');
                    throw new Exception\AlreadyExistsException("Unable to overwrite $hdfsPath ($hdfsType) with $localPath ($localType)", false);
                }
            }
            catch(Exception\NotFoundException $e)
            {
            }

            if ($isDir)
            {
                if (!$entry)
                {
                    $this->makeDir($hdfsPath);
                }
            }
            else
            {
                if ($entry)
                {
                    $this->removeFile($hdfsPath);
                }
                $this->putFile($localPath, $hdfsPath);
            }
        }
    }//function putDir()


    /**
    * Downloads $hdfsDir to $localDir.
    * If $localDir does not exist, creates it. But if parent directory of $localDir does not exist, throws exception.
    *
    * @param string $hdfsDir
    * @param string $localDir
    * @param bool $isOverwrite Overwrite existing files with the same names.
    *
    * @throws \Hdfs\Exception\IllegalArgumentException If $localDir or $hdfsDir is not a directory.
    * @throws \Hdfs\Exception\NotFoundException If $hdfsDir or parent directory of $localDir do not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to read HDFS path or unable to write to local path.
    * @throws \Hdfs\Exception\AlreadyExistsException If unable to overwrite existing files.
    */
    public function getDir ($hdfsDir, $localDir, $isOverwrite = false)
    {
        $localParent = dirname($localDir);
        if (!$this->localfs->isExists($localParent))
        {
            throw new Exception\NotFoundException("Directory does not exist: $localParent", true);
        }
        if (!$this->localfs->isDir($localParent))
        {
            throw new Exception\IllegalArgumentException("Provided path is not a directory: $localParent", true);
        }
        if (!$this->localfs->isExists($localDir))
        {
            if (!$this->localfs->isWritable($localParent))
            {
                throw new Exception\PermissionException("Unable to write to directory: $localParent", true);
            }
            $this->localfs->makeDir($localDir);
        }
        if (!$this->localfs->isDir($localDir))
        {
            throw new Exception\IllegalArgumentException("Provided path is not a direcotry: $localDir", true);
        }
        if (!$this->localfs->isWritable($localDir))
        {
            throw new Exception\PermissionException("Unable to write to directory: $localDir", true);
        }

        $localDir = preg_replace('#/$#', '', $localDir);
        $hdfsDir  = preg_replace('#/$#', '', $hdfsDir);

        $queue = $this->gatherDirInfo($hdfsDir);
        unset($queue[$hdfsDir]);

        //create directories & download files
        foreach ($queue as $hdfsPath => $isDir)
        {
            $path      = str_replace("$hdfsDir/", '', $hdfsPath);
            $localPath = "$localDir/$path";
            $exists    = $this->localfs->isExists($localPath);

            if ($exists)
            {
                if ($this->localfs->isDir($localPath) != $isDir || !$isOverwrite)
                {
                    $hdfsType = ($isDir ? 'directory' : 'file');
                    $localType = ($this->localfs->isDir($localPath) ? 'directory' : 'file');
                    throw new Exception\AlreadyExistsException("Unable to overwrite $localPath ($localType) with $hdfsPath ($hdfsType)", false);
                }
            }

            if ($isDir)
            {
                if (!$exists)
                {
                    $this->localfs->makeDir($localPath);
                }
            }
            else
            {
                if ($exists)
                {
                    $this->localfs->removeFile($localPath);
                }
                $this->getFile($hdfsPath, $localPath);
            }
        }
    }//function getDir()


    /**
    * Copy file from one HDFS location to another HDFS location.
    *
    * @param string $hdfsSrcPath
    * @param string $hdfsDstPath
    * @param bool $isOverwrite If file exists, don't throw exception, but overwrite file.
    *
    * @throws \Hdfs\Exception\NotFoundException If source path or parent directory for destination path does not exist.
    * @throws \Hdfs\Exception\PermissionException If operation is not allowed due to permissions restrictions.
    * @throws \Hdfs\Exception\AlreadyExistsException If destination path already exists and $isOverwrite = false.
    * @throws \Hdfs\Exception\IllegalArgumentException if source path or destination path is not a file.
    */
    public function copyFile ($hdfsSrcPath, $hdfsDstPath, $isOverwrite = false)
    {
        $entry = $this->stat($hdfsSrcPath);
        if (!$entry->isFile())
        {
            throw new Exception\IllegalArgumentException("Provided path is not a file: $hdfsSrcPath", false);
        }

        try
        {
            $entry = $this->stat($hdfsDstPath);
            if (!$isOverwrite)
            {
                throw new Exception\AlreadyExistsException("File already exists: $hdfsDstPath", false);
            }
            if (!$entry->isFile())
            {
                throw new Exception\IllegalArgumentException("Provided path is not a file: $hdfsDstPath", false);
            }
            if ($isOverwrite)
            {
                $this->removeFile($hdfsDstPath);
            }
        }
        catch (Exception\NotFoundException $e)
        {
        }

        $tmpDir  = sys_get_temp_dir();
        $tmpFile = "$tmpDir/". md5($hdfsDstPath);

        $this->getFile($hdfsSrcPath, $tmpFile);
        $this->putFile($tmpFile, $hdfsDstPath);
    }//function copyFile()


    /**
    * Copy all files from one HDFS location to another HDFS location.
    *
    * @param string $hdfsSrcPath
    * @param string $hdfsDstPath
    * @param bool $isOverwrite If file exists, don't throw exceptions, but overwrite files.
    *
    * @throws \Hdfs\Exception\NotFoundException If source path or parent directory for destination path does not exist.
    * @throws \Hdfs\Exception\PermissionException If operation is not allowed due to permissions restrictions.
    * @throws \Hdfs\Exception\AlreadyExistsException If destination path already exists and $isOverwrite = false.
    * @throws \Hdfs\Exception\IllegalArgumentException if source path or destination path is not a directory.
    */
    public function copyDir ($hdfsSrcPath, $hdfsDstPath, $isOverwrite = false)
    {
        $entry = $this->stat($hdfsSrcPath);
        if (!$entry->isDir())
        {
            throw new Exception\IllegalArgumentException("Provided path is not a directory: $hdfsSrcPath", false);
        }

        try
        {
            $entry = $this->stat($hdfsDstPath);
            if (!$entry->isDir())
            {
                throw new Exception\IllegalArgumentException("Provided path is not a directory: $hdfsDstPath", false);
            }
        }
        catch (Exception\NotFoundException $e)
        {
        }

        $hdfsSrcPath = preg_replace('#/$#', '', $hdfsSrcPath);
        $hdfsDstPath = preg_replace('#/$#', '', $hdfsDstPath);

        $queue = $this->gatherDirInfo($hdfsSrcPath);

        $tmpDir = sys_get_temp_dir();
        foreach ($queue as $srcPath => $isDir)
        {
            if ($srcPath == $hdfsSrcPath)
            {
                $dstPath = $hdfsDstPath;
            }
            else
            {
                $path = str_replace("$hdfsSrcPath/", '', $srcPath);
                $dstPath = "$hdfsDstPath/$path";
            }
            $entry   = null;

            try
            {
                $entry = $this->stat($dstPath);
                if ($entry->isDir() != $isDir || !$isOverwrite)
                {
                    $dstType = ($entry->isDir() ? 'directory' : 'file');
                    $srcType = ($isDir ? 'directory' : 'file');
                    throw new Exception\AlreadyExistsException("Unable to overwrite $dstPath ($dstType) with $srcPath ($srcType)", false);
                }
            }
            catch(Exception\NotFoundException $e)
            {
            }

            if ($isDir)
            {
                if (!$entry)
                {
                    $this->makeDir($dstPath);
                }
            }
            else
            {
                $tmpFile = "$tmpDir/". md5($dstPath);
                $this->getFile($srcPath, $tmpFile);

                if ($entry)
                {
                    $this->removeFile($dstPath);
                }
                $this->putFile($tmpFile, $dstPath);
            }
        }
    }//function copyDir()


    /**
    * Delete file or directory (not recursive).
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\NotFoundException If path does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to delete due to permissions restrictions.
    * @throws \Hdfs\Exception\NotEmptyException If path is directory and it's not empty.
    */
    public function delete ($hdfsPath)
    {
        $entry = $this->stat($hdfsPath);
        if ($entry->isDir())
        {
            $this->removeDir($hdfsPath);
        }
        else
        {
            $this->removeFile($hdfsPath);
        }
    }//function delete()


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
        $entry = $this->stat($hdfsPath);
        if (!$entry->isDir())
        {
            $this->removeFile($hdfsPath);
            return;
        }

        $queue = $this->gatherDirInfo($hdfsPath);
        $queue = array_reverse($queue);

        foreach ($queue as $path => $isDir)
        {
            if ($isDir)
            {
                $this->removeDir($path);
            }
            else
            {
                $this->removeFile($path);
            }
        }
    }//function deleteRecursive()


    /**
    * Get filesystem object size in bytes.
    * Always returns 0 for directories.
    *
    * @param string $hdfsPath
    *
    * @throws \Hdfs\Exception\NotFoundException If path does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to stat specified path due to permissions restrictions.
    *
    * @return int
    */
    public function getSize ($hdfsPath)
    {
        $entry = $this->stat($hdfsPath);

        return ($entry->isFile() ? $entry->getSize() : 0);
    }//function getSize()


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
        $hdfsPath = preg_replace('#/$#', '', $hdfsPath);

        $entry = $this->stat($hdfsPath);
        if (!$entry->isDir())
        {
            return $entry->getSize();
        }

        $size = 0;
        $list = array($hdfsPath => $this->readDir($hdfsPath));

        while ($list)
        {
            $base    = key($list);
            $entries = $list[$base];
            unset($list[$base]);
            reset($list);

            while ($entries)
            {
                $entry = array_shift($entries);
                $path = "$base/{$entry->getName()}";

                if ($entry->isDir())
                {
                    $list[$path]  = $this->readDir($path);
                }
                else
                {
                    $size += $entry->getSize();
                }
            }
        }

        return $size;
    }//function getSizeRecursive()


    /**
    * Get file content.
    *
    * @param string $hdfsFile
    * @param int $offset Get content starting from this position in file.
    * @param int $length Read this amount of bytes. If $length = 0, read till the end of file.
    *
    * @throws \Hdfs\Exception\IllegalArgumentException If $hdfsFile is not a file
    * @throws \Hdfs\Exception\NotFoundException If file does not exist.
    * @throws \Hdfs\Exception\PermissionException If unable to read file due to permissions restrictions.
    *
    * @return string
    */
    public function readFile ($hdfsFile, $offset = 0, $length = 0)
    {
        $tmpFile = $this->getTmpLocalFileName();
        $this->getFile($hdfsFile, $tmpFile);

        $f = $this->localfs->openFile($tmpFile, 'r');
        if ($offset)
        {
            $this->localfs->seek($f, $offset);
        }
        if (!$length)
        {
            $length = $this->localfs->size($tmpFile);
        }

        $data = $this->localfs->read($f, $length);

        $this->localfs->closeFile($f);
        $this->localfs->removeFile($tmpFile);

        return $data;
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
    */
    public function writeFile ($hdfsFile, $content)
    {
        $tmpFile = $this->getTmpLocalFileName();
        $this->localfs->saveFile($tmpFile, $content);

        try
        {
            $this->putFile($tmpFile, $hdfsFile);
        }
        catch (\Hdfs\Exception $e)
        {
            $this->localfs->removeFile($tmpFile);

            throw $e;
        }

        $this->localfs->removeFile($tmpFile);
    }//function writeFile()


    /**
    * Get list of directories/files (recursively) including $hdfsDir itself with flags, marking entry as file or directory.
    * Returns associative array of boolean values, where key is the path and value tells if this path is a directory or not.
    *
    * @param string $hdfsDir
    *
    * @return array
    */
    protected function gatherDirInfo ($hdfsDir)
    {
        $hdfsDir = preg_replace('#/$#', '', $hdfsDir);

        $list = array($hdfsDir => $this->readDir($hdfsDir));
        $info = array($hdfsDir => true);

        while ($list)
        {
            $base    = key($list);
            $entries = $list[$base];
            unset($list[$base]);
            reset($list);

            while ($entries)
            {
                $entry = array_shift($entries);
                $path = "$base/{$entry->getName()}";

                if ($entry->isDir())
                {
                    $info[$path] = true;
                    $list[$path]  = $this->readDir($path);
                }
                else
                {
                    $info[$path] = false;
                }
            }
        }

        return $info;
    }//function gatherDirInfo()


    /**
    * Tries to ceate a directory. Does not throw AlreadyExistsException if directory already exists.
    *
    * @param string $hdfsDir
    *
    * @throws \Hdfs\Exception\NotFoundException         if parent directory does not exist
    * @throws \Hdfs\Exception\PermissionException       if unable to create directory due to permission restrictions
    * @throws \Hdfs\Exception\IllegalArgumentException  if parent directory of $hdfsDir is not a directory
    */
    protected function ensureDir ($hdfsDir)
    {
        try
        {
            $this->makeDir($hdfsDir);
        }
        catch (Exception\AlreadyExistsException $e)
        {
        }
    }//function ensureDir()


    /**
    * Generate filename for temporary storage of downloaded data
    *
    * @return string
    */
    protected function getTmpLocalFileName ()
    {
        $tmpDir = sys_get_temp_dir();
        $tmpFile = $tmpDir .'/'. md5(microtime());

        return $tmpFile;
    }//function getTmpLocalFileName()


}//abstract HdfsAbstract