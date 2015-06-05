<?php

require_once __DIR__.'/../BaseCase.php';

use \Hdfs\Web\Method;


/**
* Tests for HDFS over HTTP
*
*/
class WebTest extends BaseCase {

    /** directories&files to run tests against */
    const REMOTE_DIR     = '/tmp/hdfs_cli_testDir';
    const REMOTE_DIR2    = '/tmp/hdfs_cli_testDir2';
    const REMOTE_SUBDIR  = '/tmp/hdfs_cli_testDir/sub';
    const REMOTE_SUBDIR2 = '/tmp/hdfs_cli_testDir2/sub';
    const REMOTE_FILE    = '/tmp/hdfs_cli_testDir/file.txt';
    const LOCAL_DIR      = '/tmp/hdfs_cli_local_testDir';
    const LOCAL_SUBDIR   = '/tmp/hdfs_cli_local_testDir/sub';
    const LOCAL_FILE     = '/tmp/hdfs_cli_local_testDir/file.txt';



    /**
    * Check `putFile()` when local file does not exist
    *
    */
    public function testPutFileLocalDoesNotExist ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $hdfs = new Hdfs\Web(null, $fs);

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
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isReadable')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $hdfs = new Hdfs\Web(null, $fs);

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
    * Check `putFile()` throws error if remote directory does not exist
    *
    */
    public function testPutFileRemoteDirNotFound ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(true));
        $fs->method('isReadable')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));
        $hdfs->setFilesystemWrapper($fs);

        try
        {
            $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);
            $this->assertFalse(true);
        }
        catch(Hdfs\Exception\NotFoundException $e)
        {
            $this->assertFalse($e->isLocal());
        }
    }//function testPutFileRemoteDirNotFound()


    /**
    * Check `putFile()` when no errors occur
    *
    */
    public function testPutFileSuccess ()
    {
        $datanodeUrl = 'http://example.com/'. self::REMOTE_FILE;
        $response    = $this->getWebResponse('putFile_success');

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')->with(self::LOCAL_FILE)->will($this->returnValue(true));
        $fs->method('isReadable')->with(self::LOCAL_FILE)->will($this->returnValue(true));

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('getDatanodeUrl')
            ->with(Method::PUT, self::REMOTE_FILE, 'CREATE', array('overwrite' => 'false'))
            ->will($this->returnValue($datanodeUrl));
        $web->expects($this->once())
            ->method('put')
            ->with($datanodeUrl, self::LOCAL_FILE)
            ->will($this->returnValue($response));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR);
        $hdfs->setFilesystemWrapper($fs);
        $hdfs->setWebHdfsWrapper($web);

        $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);
    }//function testPutFileSuccess()


    /**
    * Check `stat()` in case no errors occured
    *
    */
    public function testStatSuccess ()
    {
        $response = $this->getWebResponse('stat_file');
        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::GET, self::REMOTE_FILE, 'GETFILESTATUS')
            ->will($this->returnValue($response));
        $hdfs = new \Hdfs\Web($web);

        $entry = $hdfs->stat(self::REMOTE_FILE);

        $this->assertEquals(basename(self::REMOTE_FILE), $entry->getName());
        $this->assertEquals(Hdfs\EntryType::FILE, $entry->getType());
        $this->assertEquals(octdec('755'), $entry->getMode());
        $this->assertEquals(11, $entry->getSize());
        $this->assertEquals(1427994123442, $entry->getMtime());
        $this->assertTrue((bool)$entry->getOwner());
        $this->assertEquals('hdfs', $entry->getGroup());
        $this->assertEquals(3, $entry->getRfactor());
    }//function testStatSuccess()


    /**
    * Check `getFile()` throws exception if local file already exists
    *
    * @expectedException Hdfs\Exception\AlreadyExistsException
    */
    public function testGetFileLocalAlreadyExists ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isWritable')->with(self::LOCAL_DIR)->will($this->returnValue(true));

        $fs->expects($this->exactly(2))
            ->method('isExists')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_FILE, true)
                )
            ));

        $hdfs = new Hdfs\Web(null, $fs);

        $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
    }//function testGetFileLocalAlreadyExists()


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

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($entry));
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->getFile(self::REMOTE_DIR, self::LOCAL_FILE);
    }//function testGetFileNotAFile()


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

        $hdfs = new \Hdfs\Web(null, $fs);

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

        $hdfs = new \Hdfs\Web(null, $fs);

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
        $fs->expects($this->once())
            ->method('saveFile')
            ->with(self::LOCAL_FILE, 'hello, hdfs');

        $response = $this->getWebResponse('getFile_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::GET, self::REMOTE_FILE, 'OPEN')
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->method('stat')->with(self::REMOTE_FILE)->will($this->returnValue($entry));
        $hdfs->setFilesystemWrapper($fs);
        $hdfs->setWebHdfsWrapper($web);

        $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
    }//function testGetFileWithoutErrors()


    /**
    * Check `removeFile()` throws exception if file does not exists
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testRemoveFileNotFound ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('file not found', false)));

        $hdfs->removeFile(self::REMOTE_FILE);
    }//function testRemoveFileNotFound()


    /**
    * Check `removeFile()` throws exception if provided path is not a file
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testRemoveFileFailsOnNotFile ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(false));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));

        $hdfs->removeFile(self::REMOTE_FILE);
    }//function testRemoveFileFailsOnNotFile()


    /**
    * Check `removeFile()` when no errors occur
    *
    */
    public function testRemoveFileWithoutErrors ()
    {
        $response = $this->getWebResponse('remove_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::DELETE, self::REMOTE_FILE, 'DELETE')
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));
        $hdfs->setWebHdfsWrapper($web);

        $hdfs->removeFile(self::REMOTE_FILE);
    }//function testRemoveFileWithoutErrors()


    /**
    * Check `setFileReplication()` throws exception if $factor argument is invalid
    *
    */
    public function testSetFileReplicationWrongFactor ()
    {
        $hdfs = new Hdfs\Web();

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
    * Check `setFileReplication()` throws exception if file does not exists
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testSetReplicationNotFound ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('file not found', false)));

       $hdfs->setFileReplication(self::REMOTE_FILE, 10);
    }//function testSetReplicationNotFound()


    /**
    * Check `setFileReplication()` will not try to affect directories
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testSetFileReplicationFailsOnDirectory ()
    {
        $response = $this->getWebResponse('stat_directory');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::GET, self::REMOTE_DIR, 'GETFILESTATUS')
            ->will($this->returnValue($response));

        $hdfs = new \Hdfs\Web($web);

        $hdfs->setFileReplication(self::REMOTE_DIR, 10);
    }//function testSetFileReplicationFailsOnDirectory()


    /**
    * Check `setFileReplication()` when no errors occur
    *
    */
    public function testSetFileReplicationSuccess ()
    {
        $response = $this->getWebResponse('setReplication_success');
        $factor = 2;

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::PUT, self::REMOTE_FILE, 'SETREPLICATION', array('replication' => $factor))
            ->will($this->returnValue($response));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));
        $hdfs->setWebHdfsWrapper($web);

        $hdfs->setFileReplication(self::REMOTE_FILE, $factor);
    }//function testSetFileReplicationSuccess()


    /**
    * Check `makeDir()` throws exception if provided path already exist
    *
    * @expectedException \Hdfs\Exception\AlreadyExistsException
    */
    public function testMakeDirFailsIfAlreadyExists ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR);

        $hdfs->makeDir(self::REMOTE_DIR);
    }//function testMakeDirFailsIfAlreadyExists()


    /**
    * Check `makeDir()` throws exception if parent directory does not exist
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testMakeDirFailsIfParentDirNotExist ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));

        $hdfs->makeDir(self::REMOTE_SUBDIR);
    }//function testMakeDirFailsIfParentDirNotExist()


    /**
    * Check `makeDir()` when no errors occur
    *
    */
    public function testMakeDirSuccess ()
    {
        $response = $this->getWebResponse('mkdir_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::PUT, self::REMOTE_DIR, 'MKDIRS')
            ->will($this->returnValue($response));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->setWebHdfsWrapper($web);
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));

        $hdfs->makeDir(self::REMOTE_DIR);
    }//function testMakeDirSuccess()


    /**
    * Check `removeDir()` throws exception directory does not exists
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testRemoveDirNotFound ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Path not found', false)));

        $hdfs->removeDir(self::REMOTE_DIR);
    }//function testRemoveDirNotFound()


    /**
    * Check `removeDir()` throws exception if provided path is not a directory
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testRemoveDirFailsOnNotDir ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));

        $hdfs->removeDir(self::REMOTE_FILE);
    }//function testRemoveDirFailsOnNotDir()


    /**
    * Check `removeDir()` when no errors occur
    *
    */
    public function testRemoveDirSuccess ()
    {
        $response = $this->getWebResponse('remove_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::DELETE, self::REMOTE_DIR, 'DELETE')
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($entry));
        $hdfs->setWebHdfsWrapper($web);

        $hdfs->removeDir(self::REMOTE_DIR);
    }//function testRemoveDirSuccess()


    /**
    * Check `readDir()` fails if provided path is not a directory
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testReadDirFailsOnFile ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->expects($this->once())
            ->method('isDir')
            ->will($this->returnValue(false));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));

        $hdfs->readDir(self::REMOTE_FILE);
    }//function testReadDirFailsOnFile()


    /**
    * Check `readDir()` fails if provided path does not exist
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testReadDirNotExist ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));

        $hdfs->readDir(self::REMOTE_DIR);
    }//function testReadDirNotExist()


    /**
    * Check `readDir()` when no errors occur
    *
    */
    public function testReadDirSuccess ()
    {
        $response = $this->getWebResponse('liststatus_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::GET, self::REMOTE_DIR, 'LISTSTATUS')
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->method('stat')->with(self::REMOTE_DIR)->will($this->returnValue($entry));
        $hdfs->setWebHdfsWrapper($web);

        $list = $hdfs->readDir(self::REMOTE_DIR);

        $amount = count($list);
        $this->assertEquals(2, $amount);

        foreach ($list as $entry)
        {
            switch ($entry->getName())
            {
                case basename(self::REMOTE_FILE):
                        $this->assertEquals(Hdfs\EntryType::FILE, $entry->getType());
                        $this->assertEquals(octdec('644'), $entry->getMode());
                        $this->assertEquals(0, $entry->getSize());
                        $this->assertEquals(1428069362271, $entry->getMtime());
                        $this->assertTrue((bool)$entry->getOwner());
                        $this->assertEquals('hdfs', $entry->getGroup());
                        $this->assertEquals(3, $entry->getRfactor());
                    break;
                case basename(self::REMOTE_SUBDIR):
                        $this->assertEquals(Hdfs\EntryType::DIRECTORY, $entry->getType());
                        $this->assertEquals(octdec('755'), $entry->getMode());
                        $this->assertEquals(0, $entry->getSize());
                        $this->assertEquals(1428069352607, $entry->getMtime());
                        $this->assertTrue((bool)$entry->getOwner());
                        $this->assertEquals('hdfs', $entry->getGroup());
                        $this->assertEquals(0, $entry->getRfactor());
                    break;
                default:
                    $this->assertTrue(false, "Unexpected entry in readDir() result: ". json_encode($entry->toArray()));
            }
        }
    }//function testReadDirSuccess()


    /**
    * Check `changeMode()` throws exception if provided mode is invalid
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testChangeModeInvalidMode ()
    {
        $hdfs = new \Hdfs\Web();

        $hdfs->changeMode(self::REMOTE_FILE, 2.2);
    }//function testChangeModeInvalidMode()


    /**
    * Check successful `changeMode()` call
    *
    */
    public function testChangeModeSuccess ()
    {
        $mode     = 0777;
        $response = $this->getWebResponse('chmod_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::PUT, self::REMOTE_FILE, 'SETPERMISSION', array('permission' => decoct($mode)))
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Web($web);

        $entry = $hdfs->changeMode(self::REMOTE_FILE, $mode);
    }//function testChangeModeSuccess()


    /**
    * Check `changeOwner()` fails if both $owner and $group are not specified
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testChangeOwnerNotEnoughArguments ()
    {
        $hdfs = new \Hdfs\Web();

        $hdfs->changeOwner(self::REMOTE_FILE);
    }//function testChaneOwnerNotEnoughArguments()


    /**
    * Check successful `changeOwner()` call
    *
    */
    public function testChangeOwnerSuccess ()
    {
        $owner = 'test';
        $group = 'hdfs';
        $response = $this->getWebResponse('chown_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::PUT, self::REMOTE_FILE, 'SETOWNER', array('owner' => $owner, 'group' => $group))
            ->will($this->returnValue($response));

        $hdfs = new Hdfs\Web($web);

        $entry = $hdfs->changeOwner(self::REMOTE_FILE, $owner, $group);
    }//function testChangeOwnerSuccess()


    /**
    * Check `rename()` throws exception if source path does not exists
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testRenameSourceNotFound ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Path not found', false)));

        $hdfs->rename(self::REMOTE_DIR, self::REMOTE_DIR2);
    }//function testRenameSourceNotFound()


    /**
    * Check `rename()` throws exception if parent directory of destination path does not exist
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testRenameDestinationDirNotFound ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->at(0))->method('stat')->with(self::REMOTE_SUBDIR);
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_DIR2)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));

        $hdfs->rename(self::REMOTE_SUBDIR, self::REMOTE_SUBDIR2);
    }//function testRenameDestinationDirNotFound()


    /**
    * Check `rename()` throws exception if destination path already exists
    *
    * @expectedException \Hdfs\Exception\AlreadyExistsException
    */
    public function testRenameDestinationAlreadyExists ()
    {
        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->at(0))->method('stat')->with(self::REMOTE_SUBDIR);
        $hdfs->expects($this->at(1))->method('stat')->with(self::REMOTE_DIR2);
        $hdfs->expects($this->at(2))->method('stat')->with(self::REMOTE_SUBDIR2);

        $hdfs->rename(self::REMOTE_SUBDIR, self::REMOTE_SUBDIR2);
    }//function testRenameDestinationAlreadyExists()


    /**
    * Check `rename()` when no errors occur
    *
    */
    public function testRenameSuccess ()
    {
        $response = $this->getWebResponse('rename_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::PUT, self::REMOTE_SUBDIR, 'RENAME', array('destination' => self::REMOTE_SUBDIR2))
            ->will($this->returnValue($response));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->at(0))->method('stat')->with(self::REMOTE_SUBDIR);
        $hdfs->expects($this->at(1))->method('stat')->with(self::REMOTE_DIR2);
        $hdfs->expects($this->at(2))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR2)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not exist', false)));
        $hdfs->setWebHdfsWrapper($web);

        $hdfs->rename(self::REMOTE_SUBDIR, self::REMOTE_SUBDIR2);
    }//function testRenameSuccess()


    /**
    * Check `readFile()`
    *
    */
    public function testReadFile ()
    {
        $offset  = 1;
        $length  = 3;

        $response = $this->getWebResponse('getFile_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('exec')
            ->with(Method::GET, self::REMOTE_FILE, 'OPEN', array('offset' => $offset, 'length' => $length))
            ->will($this->returnValue($response));

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->method('stat')->with(self::REMOTE_FILE)->will($this->returnValue($entry));
        $hdfs->setWebHdfsWrapper($web);

        $read = $hdfs->readFile(self::REMOTE_FILE, $offset, $length);

        $expected = $response->getFileContent();
        $this->assertEquals($expected, $read);
    }//function testReadFile()


    /**
    * Check `writeFile()`
    *
    */
    public function testWriteFile ()
    {
        $tmpFileHandle = 'resource';
        $content = str_pad('q', 100);

        $datanodeUrl = 'http://example.com/'. self::REMOTE_FILE;
        $response    = $this->getWebResponse('putFile_success');

        $web = $this->getMock('\\Hdfs\\Web\\WebHdfsWrapper');
        $web->expects($this->once())
            ->method('getDatanodeUrl')
            ->with(Method::PUT, self::REMOTE_FILE, 'CREATE', array('overwrite' => 'false'))
            ->will($this->returnValue($datanodeUrl));
        $web->expects($this->once())
            ->method('put')
            ->with($datanodeUrl, $tmpFileHandle, array(), strlen($content))
            ->will($this->returnValue($response));

        $hdfs = $this->getMock('\\Hdfs\\Web', array('stat'));
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR);

        $hdfs->setWebHdfsWrapper($web);

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->at(0))->method('openFile')->with($this->anything(), 'w')->will($this->returnValue($tmpFileHandle));
        $fs->expects($this->at(1))->method('write')->with($tmpFileHandle, $content);
        $fs->expects($this->at(2))->method('seek')->with($tmpFileHandle, 0);
        $fs->expects($this->at(3))->method('closeFile')->with($tmpFileHandle);

        $hdfs->setFilesystemWrapper($fs);

        $hdfs->writeFile(self::REMOTE_FILE, $content);
    }//function testWriteFile()
}//class WebTest