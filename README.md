PHP-Hadoop-HDFS
==========
Pure PHP unified wrapper for WebHDFS and CLI `hadoop fs`.
Provides single interface for both, so you can decide which one to use depending on your tasks.

Requires PHP 5.3+

Documentation:
----------
Each method has detailed docblocks, so using phpDocumentor should be enough to get started.

API:
----------
* See docblocks for exceptions thrown by each method *

Instantiating CLI implementation:
```php
$hdfs = new \Hdfs\Cli();
```

Instantiating WebHDFS implementation:
```php
$hdfs = new \Hdfs\Web();
$hdfs->configure($host, $port, $user);
```

Change wrapper for local filesystem.
Use it if you need hdfs to interact with another remote service instead of local FS
```php
$hdfs->setFilesystemWrapper (\Hdfs\FilesystemWrapper $localfs)
```

Upload $localFile to Hadoop at $hdfsFile path
```php
$hdfs->putFile ($localFile, $hdfsFile) ;
```

Download $hdfsFile from HDFS to $localFile in local filesystem
```php
$hdfs->getFile ($hdfsFile, $localFile) ;
```

Remove file from HDFS
```php
$hdfs->removeFile ($hdfsFile) ;
```

Set replication factor of a file $hdfsFile
```php
$hdfs->setFileReplication ($hdfsFile, $factor) ;
```

Create $hdfsDir directory.
```php
$hdfs->makeDir ($hdfsDir) ;
```

Remove $hdfsDir directory.
Directory should be empty.
```php
$hdfs->removeDir ($hdfsDir) ;
```

List contents of $hdfsDir directory.
Returns array of \Hdfs\EntryStatus instances
```php
$hdfs->readDir ($hdfsDir) ;
```

Get stats for $hdfsPath
Returns \Hdfs\EntryStatus
```php
$hdfs->stat ($hdfsPath) ;
```

Change permissions for $hdfsPath
```php
$hdfs->changeMode ($hdfsPath, $mode) ;
```

Change owner and/or group for $hdfsPath.
At least two arguments should be passed to this method.
Either $owner or $group should be set.
```php
$hdfs->changeOwner ($hdfsPath, $owner = null, $group = null) ;
```

Rename $hdfsSrcPath to $hdfsDstPath
```php
$hdfs->rename ($hdfsSrcPath, $hdfsDstPath) ;
```

Check if $hdfsPath exists
```php
$hdfs->isExists ($hdfsPath)
```

Check if $hdfsPath is directory
```php
$hdfs->isDir ($hdfsPath)
```

Check if $hdfsPath is file
```php
$hdfs->isFile ($hdfsPath)
```

Create directories recursively.
If directory already exists and $mode/$owner/$group provided, then method
just tries to apply those arguments to the last component in $hdfsDir.
```php
$hdfs->makeDirRecursive ($hdfsDir, $mode = null, $owner = null, $group = null)
```

Change owner/group of $hdfsPath and all files&directories inside of $hdfsPath.
```php
$hdfs->changeOwnerRecursive ($hdfsPath, $owner = null, $group = null)
```

Change permissions of $hdfsPath and all files&directories inside of $hdfsPath.
```php
$hdfs->changeModeRecursive ($hdfsPath, $mode)
```

Uploads $localDir to $hdfsDir.
If $hdfsDir does not exist, creates it. But if parent directory of $hdfsDir does not exist, throws exception.
```php
$hdfs->putDir ($localDir, $hdfsDir, $isOverwrite = false)
```

Downloads $hdfsDir to $localDir.
If $localDir does not exist, creates it. But if parent directory of $localDir does not exist, throws exception.
```php
$hdfs->getDir ($hdfsDir, $localDir, $isOverwrite = false)
```

Copy file from one HDFS location to another HDFS location.
```php
$hdfs->copyFile ($hdfsSrcPath, $hdfsDstPath, $isOverwrite = false)
```

Copy all files from one HDFS location to another HDFS location.
```php
$hdfs->copyDir ($hdfsSrcPath, $hdfsDstPath, $isOverwrite = false)
```

Delete file or directory (not recursive).
```php
$hdfs->delete ($hdfsPath)
```

Delete file or directory recursively.
```php
$hdfs->deleteRecursive ($hdfsPath)
```

Get filesystem object size in bytes.
Always returns 0 for directories.
```php
$hdfs->getSize ($hdfsPath)
```

Get filesystem object size in bytes.
Recursively gathers size information for directories.
```php
$hdfs->getSizeRecursive ($hdfsPath)
```

Get file content without saving it to local filesystem.
```php
$content = $hdfs->readFile ($hdfsFile, $offset = 0, $length = 0);
```

Write content to file.
```php
$hdfs->writeFile ($hdfsFile, $content)
```