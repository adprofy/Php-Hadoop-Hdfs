<?php

require_once __DIR__.'/../BaseCase.php';


/**
* Test methods of CLI-based HDFS client implementation
*
*/
class CliTest extends BaseCase
{
    /** directories&files to run tests against */
    const REMOTE_DIR    = '/tmp/hdfs_cli_testDir';
    const REMOTE_SUBDIR = '/tmp/hdfs_cli_testDir/sub';
    const REMOTE_FILE   = '/tmp/hdfs_cli_testDir/file.txt';
    const LOCAL_DIR     = '/tmp/hdfs_cli_local_testDir';
    const LOCAL_SUBDIR  = '/tmp/hdfs_cli_local_testDir/sub';
    const LOCAL_FILE    = '/tmp/hdfs_cli_local_testDir/file.txt';


    /**
    * Check `isExists()` on existing path
    *
    */
    public function testisExistsPathExists ()
    {
        $response = $this->getCliResponse('isExists_true');
        $mock     = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $mock->expects($this->once())
            ->method('exec')
            ->with('-test', '-e', self::REMOTE_DIR)
            ->will($this->returnValue($response));
        $hdfs = new Hdfs\Cli($mock);

        $exists = $hdfs->isExists(self::REMOTE_DIR);

        $this->assertTrue($exists);
    }//function testisExistsPathExists()


    /**
    * Check `isExists()` on non-existent path
    *
    */
    public function testisExistsPathDoesNotExist ()
    {
        $response = $this->getCliResponse('isExists_false');
        $cli      = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-test', '-e', self::REMOTE_DIR)
            ->will($this->returnValue($response));
        $hdfs = new Hdfs\Cli($cli);

        $exists = $hdfs->isExists(self::REMOTE_DIR);

        $this->assertFalse($exists);
    }//function testisExistsPathDoesNotExist()


    /**
    * Check `putFile()` when local file does not exist
    *
    */
    public function testPutFileLocalDoesNotExist ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $hdfs = new Hdfs\Cli(null, $fs);

        try
        {
            $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);
            $this->assertFalse(true);
        }
        catch(Hdfs\Exception\NotFoundException $e)
        {
            $this->assertTrue($e->isLocal());
        }
    }//function testPutFileLocalDoesNotExist()


    /**
    * Check `putFile()` throws exception due to permission restrictions on local path
    *
    */
    public function testPutFileLocalPermissionDenied ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(true));
        $fs->method('isReadable')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $hdfs = new Hdfs\Cli(null, $fs);

        try
        {
            $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);
            $this->assertFalse(true);
        }
        catch(Hdfs\Exception\PermissionException $e)
        {
            $this->assertTrue($e->isLocal());
        }
    }//function testPutFileLocalPermissionDenied()


    /**
    * Check `putFile()` when no errors happen
    *
    */
    public function testPutFileWithoutErrors ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isReadable')->will($this->returnValue(true));
        $fs->method('isExists')->will($this->returnValue(true));

        $response = $this->getCliResponse('putFile_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-put', self::LOCAL_FILE, self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli, $fs);

        $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);
    }//function testPutFileWithoutErrors()


    /**
    * Check `getFile()` throws exception if local file already exists
    *
    * @expectedException Hdfs\Exception\AlreadyExistsException
    */
    public function testGetFileLocalAlreadyExists ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isWritable')->with(self::LOCAL_DIR)->will($this->returnValue(true));

        $fs->expects($this->atLeastOnce())
            ->method('isExists')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_FILE, true)
                )
            ));

        $hdfs = new Hdfs\Cli(null, $fs);

        $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
    }//function testGetFileLocalAlreadyExists()


    /**
    * Check `getFile()` throws exception if directory for local file does not exist
    *
    */
    public function testGetFileLocalDirectoryDoesNotExist ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));

        $hdfs = new \Hdfs\Cli(null, $fs);

        try
        {
            $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
            $this->assertTrue(false);
        }
        catch(Hdfs\Exception\NotFoundException $e)
        {
            $this->assertTrue($e->isLocal());
        }
    }//function testGetFileLocalDirectoryDoesNotExist()


    /**
    * Check `getFile()` throws exception if provided path is not a file
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testGetFileNotAFile ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isWritable')->with(self::LOCAL_DIR)->will($this->returnValue(true));
        $fs->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(self::LOCAL_DIR, true),
                        array(self::LOCAL_FILE, false)
                    )
                )
            );

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(false));

        $hdfs = $this->getMock('\\Hdfs\\Cli', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($entry));
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->getFile(self::REMOTE_DIR, self::LOCAL_FILE);
    }//function testGetFileNotAFile()


    /**
    * Check `getFile()` when no errors happen
    *
    */
    public function testGetFileWithoutErrors ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isWritable')->will($this->returnValue(true));
        $fs->method('isExists')->will(
            $this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_FILE, false)
                )
            )
        );

        $response = $this->getCliResponse('getFile_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-get', self::REMOTE_FILE, self::LOCAL_FILE)
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Cli', array('stat'));
        $hdfs->method('stat')->with(self::REMOTE_FILE)->will($this->returnValue($entry));
        $hdfs->setFilesystemWrapper($fs);
        $hdfs->setHadoopWrapper($cli);

        $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
    }//function testGetFileWithoutErrors()


    /**
    * Check `getFile()` throws exception due to permission restrictions on local path
    *
    */
    public function testGetFileLocalPermissionDenied ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')->with(self::LOCAL_DIR)->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isWritable')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));

        $hdfs = new \Hdfs\Cli(null, $fs);

        try
        {
            $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
            $this->assertTrue(false);
        }
        catch(Hdfs\Exception\PermissionException $e)
        {
            $this->assertTrue($e->isLocal());
        }
    }//function testGetFileLocalPermissionDenied()


    /**
    * Check `removeFile()` when no errors occur
    *
    */
    public function testRemoveFileWithoutErrors ()
    {
        $response = $this->getCliResponse('removeFile_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-rm', '-skipTrash', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new \Hdfs\Cli($cli);

        $hdfs->removeFile(self::REMOTE_FILE);
    }//function testRemoveFileWithoutErrors()


    /**
    * Check `setFileReplication()` throws exception if $factor argument is invalid
    *
    */
    public function testSetFileReplicationWrongFactor ()
    {
        $hdfs = new Hdfs\Cli();

        try
        {
            $hdfs->setFileReplication(self::REMOTE_DIR, 'invalid factor');
            $this->assertTrue(false);
        }
        catch(Hdfs\Exception\IllegalArgumentException $e)
        {
            $this->assertTrue(true);
        }

        try
        {
            $hdfs->setFileReplication(self::REMOTE_DIR, -1);
            $this->assertTrue(false);
        }
        catch(Hdfs\Exception\IllegalArgumentException $e)
        {
            $this->assertTrue(true);
        }

        try
        {
            $hdfs->setFileReplication(self::REMOTE_DIR, 1.1);
            $this->assertTrue(false);
        }
        catch(Hdfs\Exception\IllegalArgumentException $e)
        {
            $this->assertTrue(true);
        }
    }//function testSetFileReplicationWrongFactor()


    /**
    * Check `setFileReplication()` will not try to affect whole directories
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testSetFileReplicationFailsOnDirectory ()
    {
        $response = $this->getCliResponse('is_directory_true');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-test', '-d', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->setFileReplication(self::REMOTE_DIR, 10);
    }//function testSetFileReplicationFailsOnDirectory()


    /**
    * Check `setFileReplication()` when no errors happen
    *
    */
    public function testSetFileReplicationWithoutErrors ()
    {
        $factor = 10;
        $responseIsNotDirectory = $this->getCliResponse('is_directory_false');
        $responseSetReplication = $this->getCliResponse('setReplication_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->exactly(2))
            ->method('exec')
            ->will(
                $this->returnValueMap(
                    array(
                        array('-test', '-d', self::REMOTE_FILE, $responseIsNotDirectory),
                        array('-setrep', $factor, self::REMOTE_FILE, $responseSetReplication)
                    )
                )
            );

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->setFileReplication(self::REMOTE_FILE, 10);
    }//function testSetFileReplicationWithoutErrors()


    /**
    * Check `makeDir()` will throw correct exception if performed on existing file
    *
    * @expectedException \Hdfs\Exception\AlreadyExistsException
    */
    public function testMakeDirOnFileAlreadyExists ()
    {
        $response = $this->getCliResponse('is_not_directory');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-mkdir', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->makeDir(self::REMOTE_DIR);
    }//function testMakeDirOnFileAlreadyExists()


    /**
    * Check `makeDir()` will throw correct exception if parent directory is actualy a file.
    * For this test we pretend that self::REMOTE_DIR is a file.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testMakeDirWhenParentIsFile()
    {
        $response = $this->getCliResponse('is_not_directory_parent');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-mkdir', self::REMOTE_SUBDIR)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->makeDir(self::REMOTE_SUBDIR);
    }//function testMakeDirWhenParentIsFile()


    /**
    * Check `makeDir()` when directory is created successfully
    *
    */
    public function testMakeDirSuccessfully ()
    {
        $response = $this->getCliResponse('mkdir_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-mkdir', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->makeDir(self::REMOTE_DIR);
    }//function testMakeDirSuccessfully()


    /**
    * Check `removeDir()` removes directory
    *
    */
    public function testRemoveDirSuccessfully ()
    {
        $response = $this->getCliResponse('rmdir_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-rmdir', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->removeDir(self::REMOTE_DIR);
    }//function testRemoveDirSuccessfully()


    /**
    * Check `readDir()` throws exception if specified path is not a directory
    *
    * @expectedException Hdfs\Exception\IllegalArgumentException
    */
    public function testReadDirNotDirectory ()
    {
        $response = $this->getCliResponse('ls_on_file');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-ls', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $hdfs->readDir(self::REMOTE_FILE);
    }//function testReadDirNotDirectory()


    /**
    * Check `readDir()` when no errors occur
    *
    */
    public function testReadDirWithoutErrors ()
    {
        $response = $this->getCliResponse('ls_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-ls', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $list = $hdfs->readDir(self::REMOTE_DIR);

        $amount = count($list);
        $this->assertEquals(2, $amount);

        foreach ($list as $entry)
        {
            switch ($entry->getName())
            {
                case basename(self::REMOTE_FILE):
                        $this->assertEquals(Hdfs\EntryType::FILE, $entry->getType());
                        $this->assertEquals(octdec('755'), $entry->getMode());
                        $this->assertEquals(0, $entry->getSize());
                        $this->assertEquals(strtotime('2015-04-01 15:47:00'), $entry->getMtime());
                        $this->assertTrue((bool)$entry->getOwner());
                        $this->assertEquals('hdfs', $entry->getGroup());
                        $this->assertEquals(3, $entry->getRfactor());
                    break;
                case basename(self::REMOTE_SUBDIR):
                        $this->assertEquals(Hdfs\EntryType::DIRECTORY, $entry->getType());
                        $this->assertEquals(octdec('644'), $entry->getMode());
                        $this->assertEquals(0, $entry->getSize());
                        $this->assertEquals(strtotime('2015-04-01 15:45:00'), $entry->getMtime());
                        $this->assertTrue((bool)$entry->getOwner());
                        $this->assertEquals('hdfs', $entry->getGroup());
                        $this->assertEquals(0, $entry->getRfactor());
                    break;
                default:
                    $this->assertTrue(false, "Unexpected entry in readDir() result: ". json_encode($entry->toArray()));
            }
        }
    }//function testReadDirWithoutErrors()


    /**
    * Check successful `stat()` call
    *
    */
    public function testStatSuccess ()
    {
        $response = $this->getCliResponse('ls_on_file');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-ls', '-d', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $entry = $hdfs->stat(self::REMOTE_FILE);

        $this->assertEquals(basename(self::REMOTE_FILE), $entry->getName());
        $this->assertEquals(Hdfs\EntryType::FILE, $entry->getType());
        $this->assertEquals(octdec('755'), $entry->getMode());
        $this->assertEquals(0, $entry->getSize());
        $this->assertEquals(strtotime('2015-04-01 15:47:00'), $entry->getMtime());
        $this->assertTrue((bool)$entry->getOwner());
        $this->assertEquals('hdfs', $entry->getGroup());
        $this->assertEquals(1, $entry->getRfactor());
    }//function testStatSuccess()


    /**
    * Check successful `changeMode()` call
    *
    */
    public function testChangeModeSuccess ()
    {
        $response = $this->getCliResponse('chmod_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-chmod', '777', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $entry = $hdfs->changeMode(self::REMOTE_FILE, 0777);
    }//function testChangeModeSuccess()


    /**
    * Check `changeMode()` throws exception if provided mode is invalid
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testChangeModeInvalidMode ()
    {
        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $hdfs = new \Hdfs\Cli($cli);

        $hdfs->changeMode(self::REMOTE_FILE, 2.2);
    }//function testChangeModeInvalidMode()


    /**
    * Check successful `changeOwner()` call
    *
    */
    public function testChangeOwnerSuccess ()
    {
        $response = $this->getCliResponse('chmod_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-chown', 'root:hdfs', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $entry = $hdfs->changeOwner(self::REMOTE_FILE, 'root', 'hdfs');
    }//function testChangeOwnerSuccess()


    /**
    * Check `changeOwner()` fails if $owner and $group are not specified
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testChangeOwnerNotEnoughArguments ()
    {
        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $hdfs = new \Hdfs\Cli($cli);

        $hdfs->changeOwner(self::REMOTE_FILE);
    }//function testChaneOwnerNotEnoughArguments()


    /**
    * Check successful `rename()` execution
    *
    */
    public function testRenameSuccess ()
    {
        $response = $this->getCliResponse('mv_success');
        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-mv', self::REMOTE_SUBDIR, self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new \Hdfs\Cli($cli);

        $hdfs->rename(self::REMOTE_SUBDIR, self::REMOTE_DIR);
    }//function testRenameSuccess()


    /**
    * Check `getMerge()` throws exception if destination file already exists
    *
    * @expectedException \Hdfs\Exception\AlreadyExistsException
    */
    public function testGetMergeAlreadyExists ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(true));

        $hdfs = new \Hdfs\Cli(null, $fs);

        $hdfs->getMerge(self::REMOTE_DIR, self::LOCAL_FILE);
    }//function testGetMergeAlreadyExists()


    /**
    * Check `getMerge()` throws exception if directory for destination file does not exist
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testGetMergeDirectoryNotFound ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->at(0))
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $fs->expects($this->at(1))
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));

        $hdfs = new \Hdfs\Cli(null, $fs);

        $hdfs->getMerge(self::REMOTE_DIR, self::LOCAL_FILE);
    }//function testGetMergeDirectoryNotFound()


    /**
    * Check `getMerge()` throws exception if directory for destination file is not writable
    *
    * @expectedException \Hdfs\Exception\PermissionException
    */
    public function testGetMergeDirectoryPermissionDenied ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->at(0))
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $fs->expects($this->at(1))
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isWritable')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));
        $hdfs = new \Hdfs\Cli(null, $fs);

        $hdfs->getMerge(self::REMOTE_DIR, self::LOCAL_FILE);
    }//function testGetMergeDirectoryPermissionDenied()


    /**
    * Check `getMerge()` when no errors occur
    *
    */
    public function testGetMergeSuccess ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->at(0))
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $fs->expects($this->at(1))
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isWritable')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('removeFile')
            ->with(self::LOCAL_DIR .'.'. basename(self::LOCAL_FILE) .'.crc');

        $response = $this->getCliResponse('getmerge_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-getmerge', '-nl', self::REMOTE_DIR, self::LOCAL_FILE)
            ->will($this->returnValue($response));

        $hdfs = new \Hdfs\Cli($cli, $fs);

        $hdfs->getMerge(self::REMOTE_DIR, self::LOCAL_FILE, true, true);
    }//function testGetMergeSuccess()


    /**
    * Check successful `changeOwnerRecursive()` call
    *
    */
    public function testChangeOwnerRecursiveSuccess ()
    {
        $response = $this->getCliResponse('chmod_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-chown', '-R', 'root:hdfs', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $entry = $hdfs->changeOwnerRecursive(self::REMOTE_FILE, 'root', 'hdfs');
    }//function testChangeOwnerRecursiveSuccess()


    /**
    * Check successful `changeModeRecursive()` call
    *
    */
    public function testChangeModeRecursiveSuccess ()
    {
        $response = $this->getCliResponse('chmod_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-chmod', '-R', '777', self::REMOTE_FILE)
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Cli($cli);

        $entry = $hdfs->changeModeRecursive(self::REMOTE_FILE, 0777);
    }//function testChangeModeRecursiveSuccess()


    /**
    * Check `makeDirRecursive()` when directory is created successfully
    *
    */
    public function testMakeDirRecursiveSuccessfully ()
    {
        $mode  = 0777;
        $user  = 'test';
        $group = 'hdfs';

        $response = $this->getCliResponse('mkdir_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-mkdir', '-p', self::REMOTE_SUBDIR)
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Cli', array('stat', 'changeModeRecursive', 'changeOwnerRecursive'));
        $hdfs->setHadoopWrapper($cli);
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));
        $hdfs->expects($this->at(2))
            ->method('stat')
            ->with('/tmp')
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('changeModeRecursive')
            ->with(self::REMOTE_DIR, $mode);
        $hdfs->expects($this->once())
            ->method('changeOwnerRecursive')
            ->with(self::REMOTE_DIR, $user, $group);

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR, $mode, $user, $group);
    }//function testMakeDirRecursiveSuccessfully()


    /**
    * Check `makeDirRecursive()` when directory already exists
    *
    */
    public function testMakeDirRecursiveAlreadyExists ()
    {
        $mode  = 0777;
        $user  = 'test';
        $group = 'hdfs';

        $response = $this->getCliResponse('mkdir_success');

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Cli', array('stat', 'changeModeRecursive', 'changeOwnerRecursive'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('changeModeRecursive')
            ->with(self::REMOTE_SUBDIR, $mode);
        $hdfs->expects($this->once())
            ->method('changeOwnerRecursive')
            ->with(self::REMOTE_SUBDIR, $user, $group);

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR, $mode, $user, $group);
    }//function testMakeDirRecursiveAlreadyExists()


    /**
    * Check `deleteRecursive()` when no errors occur
    *
    */
    public function testDeleteRecursiveWithoutErrors ()
    {
        $response = $this->getCliResponse('removeFile_success');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-rm', '-r', '-skipTrash', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new \Hdfs\Cli($cli);

        $hdfs->deleteRecursive(self::REMOTE_DIR);
    }//function testDeleteRecursiveWithoutErrors()


    /**
    * Check `getSizeRecursive()` when no errors occur
    *
    */
    public function testGetSizeRecursiveWithoutErrors ()
    {
        $response = $this->getCliResponse('count');

        $cli = $this->getMock('\\Hdfs\\Cli\\HadoopWrapper');
        $cli->expects($this->once())
            ->method('exec')
            ->with('-count', self::REMOTE_DIR)
            ->will($this->returnValue($response));

        $hdfs = new \Hdfs\Cli($cli);

        $size = $hdfs->getSizeRecursive(self::REMOTE_DIR);

        $this->assertEquals(15, $size);
    }//function testGetSizeRecursiveWithoutErrors()

}//class CliTest