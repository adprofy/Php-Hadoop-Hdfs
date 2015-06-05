<?php


require_once __DIR__.'/../BaseCase.php';


/**
* Tests for higher-level hdfs methods
*
*/
class HdfsAbstractTest extends BaseCase {

    /** directories&files to run tests against */
    const REMOTE_DIR     = '/tmp/hdfs_cli_testDir';
    const REMOTE_DIR2    = '/tmp/hdfs_cli_testDir2';
    const REMOTE_SUBDIR  = '/tmp/hdfs_cli_testDir/sub';
    const REMOTE_SUBDIR2 = '/tmp/hdfs_cli_testDir2/sub';
    const REMOTE_FILE    = '/tmp/hdfs_cli_testDir/file.txt';
    const REMOTE_FILE2   = '/tmp/hdfs_cli_testDir2/file.txt';
    const LOCAL_DIR      = '/tmp/hdfs_cli_local_testDir';
    const LOCAL_SUBDIR   = '/tmp/hdfs_cli_local_testDir/sub';
    const LOCAL_FILE     = '/tmp/hdfs_cli_local_testDir/file.txt';


    /**
    * Check `isExists()` returns `true` on existing path
    *
    */
    public function testIsExistsOnExistingPath ()
    {
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())->method('stat')->with(self::REMOTE_SUBDIR);

        $exists = $hdfs->isExists(self::REMOTE_SUBDIR);

        $this->assertTrue($exists);
    }//function testIsExistsOnExistingPath()


    /**
    * Check `isExists()` returns `false` on non-existent path
    *
    */
    public function testIsExistsOnNonExistentPath ()
    {
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Path not found', false)));

        $exists = $hdfs->isExists(self::REMOTE_SUBDIR);

        $this->assertFalse($exists);
    }//function testIsExistsOnNonExistentPath()


    /**
    * Check `makeDirRecursive()` if directory already exists and no $mode/$group/$owner provided
    *
    */
    public function testMakeDirRecursiveIfDirAlreadyExists ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->never())->method('changeMode');
        $hdfs->expects($this->never())->method('changeOwner');
        $hdfs->expects($this->never())->method('makeDir');

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR);
    }//function testMakeDirRecursiveIfDirAlreadyExists ()


    /**
    * Check `makeDirRecursive()` if directory already exists and new permissions mode is provided.
    *
    */
    public function testMakeDirRecursiveIfDirAlreadyExistsAndModeProvided ()
    {
        $mode  = 0777;

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('changeMode')
            ->with(self::REMOTE_SUBDIR, $mode);
        $hdfs->expects($this->never())->method('changeOwner');
        $hdfs->expects($this->never())->method('makeDir');

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR, $mode);
    }//function testMakeDirRecursiveIfDirAlreadyExistsAndModeProvided()


    /**
    * Check `makeDirRecursive()` if directory already exists and $owner/$group provided.
    *
    */
    public function testMakeDirRecursiveIfDirAlreadyExistsAndOwnerGroupProvided ()
    {
        $owner = 'test';
        $group = 'hdfsAbstract';

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('changeOwner')
            ->with(self::REMOTE_SUBDIR, $owner, $group);
        $hdfs->expects($this->never())->method('changeMode');
        $hdfs->expects($this->never())->method('makeDir');

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR, null, $owner, $group);
    }//function testMakeDirRecursiveIfDirAlreadyExistsAndOwnerGroupProvided()


    /**
    * Check `makeDirRecusrive()` throws exception if provided path exists and it's not a directory
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testMakeDirRecursiveFailsOnFile ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));

        $hdfs->makeDirRecursive(self::REMOTE_FILE);
    }//function testMakeDirRecursiveFailsOnFile()


    /**
    * Check `makeDirRecursive()` subsequently creates directories for provided path when $mode/$owner/$group are not provided
    *
    */
    public function testMakeDirRecursiveCreatesNestedDirsWithoutModeOrOwner ()
    {
        $existingPart = dirname(self::REMOTE_DIR);

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));
        $hdfs->expects($this->at(2))
            ->method('stat')
            ->with($existingPart)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->at(3))
            ->method('makeDir')
            ->with(self::REMOTE_DIR);
        $hdfs->expects($this->at(4))
            ->method('makeDir')
            ->with(self::REMOTE_SUBDIR);
        $hdfs->expects($this->never())->method('changeMode');
        $hdfs->expects($this->never())->method('changeOwner');

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR);
    }//function testMakeDirRecursiveCreatesNestedDirsWithoutModeOrOwner()


    /**
    * Check `makeDirRecursive()` subsequently creates directories for provided path when $mode/$owner/$group are specified
    *
    */
    public function testMakeDirRecursiveCreatesNestedDirsWithModeOrOwner ()
    {
        $existingPart = dirname(self::REMOTE_DIR);
        $mode  = 0777;
        $owner = 'test';
        $group = 'hdfsAbstract';

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));
        $hdfs->expects($this->at(2))
            ->method('stat')
            ->with($existingPart)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->at(3))
            ->method('makeDir')
            ->with(self::REMOTE_DIR);
        $hdfs->expects($this->at(4))
            ->method('changeMode')
            ->with(self::REMOTE_DIR, $mode);
        $hdfs->expects($this->at(5))
            ->method('changeOwner')
            ->with(self::REMOTE_DIR, $owner, $group);
        $hdfs->expects($this->at(6))
            ->method('makeDir')
            ->with(self::REMOTE_SUBDIR);
        $hdfs->expects($this->at(7))
            ->method('changeMode')
            ->with(self::REMOTE_SUBDIR, $mode);
        $hdfs->expects($this->at(8))
            ->method('changeOwner')
            ->with(self::REMOTE_SUBDIR, $owner, $group);

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR, $mode, $owner, $group);
    }//function testMakeDirRecursiveCreatesNestedDirsWithModeOrOwner()


    /**
    * Check `makeDirectoryRecursive()` throws exception if the last of existing components of provided path is not a directory
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testMakeDirectoryRecursiveFailsOnFileInTheMiddle ()
    {
        $entryFile = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryFile->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($entryFile));

        $hdfs->makeDirRecursive(self::REMOTE_SUBDIR);
    }//function testMakeDirectoryRecursiveFailsOnFileInTheMiddle()


    /**
    * Check `changeOwnerRecursive()` on non-existent path
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testChangeOwnerRecursiveNotFound ()
    {
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));

        $hdfs->changeOwnerRecursive(self::REMOTE_DIR, 'test', 'hdfsAbstract');
    }//function testChangeOwnerRecursiveNotFound()


    /**
    * Check `changeOwnerRecursive()` throws exception if both $owner and $group are not set.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testChangeOwnerRecursiveOwnerAndGroupNotSet ()
    {
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');

        $hdfs->changeOwnerRecursive(self::REMOTE_DIR);
    }//function testChangeOwnerRecursiveOwnerAndGroupNotSet()


    /**
    * Check `changeOwnerRecursive()` applied to single file.
    *
    */
    public function testChangeOwnerRecursiveOnFile ()
    {
        $owner = 'test';
        $group = 'hdfsAbstract';

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('changeOwner')
            ->with(self::REMOTE_FILE, $owner, $group);

        $hdfs->changeOwnerRecursive(self::REMOTE_FILE, $owner, $group);
    }//function testChangeOwnerRecursiveOnFile()


    /**
    * Check `changeOwnerRecursive()` apply owner/group to all files & subdirectories
    *
    */
    public function testChangeOwnerRecursiveOnDir ()
    {
        $owner = 'test';
        $group = 'hdfsAbstract';

        $root = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $root->method('isDir')->will($this->returnValue(true));

        $level1 = array(
            $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock(),
            $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock()
        );
        //subdir
        $level1[0]->method('isDir')->will($this->returnValue(true));
        $level1[0]->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        //file
        $level1[1]->method('isDir')->will($this->returnValue(false));
        $level1[1]->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));

        //subdir is empty
        $level2 = array();

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($root));
        $hdfs->expects($this->at(1))
            ->method('changeOwner')
            ->with(self::REMOTE_DIR, $owner, $group);
        $hdfs->expects($this->at(2))
            ->method('readDir')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($level1));
        $hdfs->expects($this->at(3))
            ->method('changeOwner')
            ->with(self::REMOTE_SUBDIR, $owner, $group);
        $hdfs->expects($this->at(4))
            ->method('readDir')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($level2));
        $hdfs->expects($this->at(5))
            ->method('changeOwner')
            ->with(self::REMOTE_FILE, $owner, $group);

        $hdfs->changeOwnerRecursive(self::REMOTE_DIR, $owner, $group);
    }//function testChangeOwnerRecursiveOnDir()


    /**
    * Check `changeModeRecursive()` on non-existent path
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testChangeModeRecursiveNotFound ()
    {
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->throwException(new \Hdfs\Exception\NotFoundException('Dir not found', false)));

        $hdfs->changeModeRecursive(self::REMOTE_DIR, 0777);
    }//function testChangeModeRecursiveNotFound()


    /**
    * Check `changeModeRecursive()` throws exception if $mode is not integer.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testChangeModeRecursiveOwnerAndGroupNotSet ()
    {
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');

        $hdfs->changeModeRecursive(self::REMOTE_DIR, 'invalid mode');
    }//function testChangeModeRecursiveOwnerAndGroupNotSet()


    /**
    * Check `changeModeRecursive()` applied to single file.
    *
    */
    public function testChangeModeRecursiveOnFile ()
    {
        $mode = 0777;

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('changeMode')
            ->with(self::REMOTE_FILE, $mode);

        $hdfs->changeModeRecursive(self::REMOTE_FILE, $mode);
    }//function testChangeModeRecursiveOnFile()


    /**
    * Check `changeModeRecursive()` apply owner/group to all files & subdirectories
    *
    */
    public function testChangeModeRecursiveOnDir ()
    {
        $mode = 0777;
        $root = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $root->method('isDir')->will($this->returnValue(true));

        $level1 = array(
            $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock(),
            $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock()
        );
        //subdir
        $level1[0]->method('isDir')->will($this->returnValue(true));
        $level1[0]->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        //file
        $level1[1]->method('isDir')->will($this->returnValue(false));
        $level1[1]->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));

        //subdir is empty
        $level2 = array();

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($root));
        $hdfs->expects($this->at(1))
            ->method('changeMode')
            ->with(self::REMOTE_DIR, $mode);
        $hdfs->expects($this->at(2))
            ->method('readDir')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($level1));
        $hdfs->expects($this->at(3))
            ->method('changeMode')
            ->with(self::REMOTE_SUBDIR, $mode);
        $hdfs->expects($this->at(4))
            ->method('readDir')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($level2));
        $hdfs->expects($this->at(5))
            ->method('changeMode')
            ->with(self::REMOTE_FILE, $mode);

        $hdfs->changeModeRecursive(self::REMOTE_DIR, $mode);
    }//function testChangeModeRecursiveOnDir()


    /**
    * Check `putDir()` throws exception if local path does not exist.
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testPutDirLocalDirNotFound ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_DIR, self::REMOTE_DIR);
    }//function testPutDirLocalDirNotFound()


    /**
    * Check `putDir()` throws exception if local path is not a directory.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testPutDirLocalPathIsNotDir ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isDir')
            ->with(self::LOCAL_FILE)
            ->will($this->returnValue(false));
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_FILE, self::REMOTE_DIR);
    }//function testPutDirLocalPathIsNotDir()


    /**
    * Check `putDir()` throws exception if local path could not be read.
    *
    * @expectedException \Hdfs\Exception\PermissionException
    */
    public function testPutDirLocalPermissionDenied ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isDir')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isReadable')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_DIR, self::REMOTE_DIR);
    }//function testPutDirLocalPermissionDenied()


    /**
    * Check `putDir()` throws exception if parent directory for $hdfsDir does not exist
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testPutDirRemoteParentDirDoesNotExist ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isDir')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('isReadable')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(true));
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract', array(), '', true, true, true, array('isExists'));
        $hdfs->expects($this->once())
            ->method('isExists')
            ->with(dirname(self::REMOTE_DIR))
            ->will($this->returnValue(false));
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_DIR, self::REMOTE_DIR);
    }//function testPutDirRemoteParentDirDoesNotExist()


    /**
    * Check `putDir()` without any overwrites
    *
    */
    public function testPutDirNoOverwrites ()
    {
        $level1 = array(self::LOCAL_SUBDIR, self::LOCAL_FILE);
        $level2 = array();

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')->with(self::LOCAL_DIR)->will($this->returnValue(true));
        $fs->method('isDir')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_SUBDIR, true),
                    array(self::LOCAL_FILE, false)
                )
            ));
        $fs->method('isReadable')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_SUBDIR, true),
                    array(self::LOCAL_FILE, true)
                )
            ));
        $fs->method('readDir')
            ->will(
                $this->returnValueMap(
                    array(
                        array(self::LOCAL_DIR, $level1),
                        array(self::LOCAL_SUBDIR, $level2)
                    )
                )
            );
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract', array(), '', true, true, true, array('isExists'));
        $hdfs->expects($this->exactly(2))
            ->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(dirname(self::REMOTE_DIR), true),
                        array(self::REMOTE_DIR, false)
                    )
                )
            );
        $hdfs->expects($this->exactly(2))
            ->method('makeDir')
            ->will(
                $this->returnValueMap(
                    array(
                        array(self::REMOTE_DIR, null),
                        array(self::REMOTE_SUBDIR, null)
                    )
                )
            );
        $hdfs->method('stat')->will($this->throwException(new \Hdfs\Exception\NotFoundException('Path not found', false)));
        $hdfs->expects($this->once())
            ->method('putFile')
            ->with(self::LOCAL_FILE, self::REMOTE_FILE);
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_DIR, self::REMOTE_DIR);
    }//function testPutDirNoOverwrites()


    /**
    * Check `putDir()` without any overwrites
    *
    */
    public function testPutDirWithOverwrites ()
    {
        $level1 = array(self::LOCAL_SUBDIR, self::LOCAL_FILE);
        $level2 = array();

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')->with(self::LOCAL_DIR)->will($this->returnValue(true));
        $fs->method('isDir')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_SUBDIR, true),
                    array(self::LOCAL_FILE, false)
                )
            ));
        $fs->method('isReadable')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_SUBDIR, true),
                    array(self::LOCAL_FILE, true)
                )
            ));
        $fs->method('readDir')
            ->will(
                $this->returnValueMap(
                    array(
                        array(self::LOCAL_DIR, $level1),
                        array(self::LOCAL_SUBDIR, $level2)
                    )
                )
            );
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract', array(), '', true, true, true, array('isExists'));
        $hdfs->expects($this->exactly(2))
            ->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(dirname(self::REMOTE_DIR), true),
                        array(self::REMOTE_DIR, true)
                    )
                )
            );
        $hdfs->expects($this->never())->method('makeDir');

        $entryDir = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryDir->method('isDir')->will($this->returnValue(true));
        $entryFile = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryFile->method('isDir')->will($this->returnValue(false));

        $ethis = $this;
        $hdfs->expects($this->exactly(2))
            ->method('stat')
            ->will(
                $this->returnCallback(function($path) use ($ethis) {
                    $isDir = HdfsAbstractTest::REMOTE_SUBDIR == $path;
                    $entry = $ethis->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
                    $entry->method('isDir')->will($ethis->returnValue($isDir));
                    return $entry;
                })
            );
        $hdfs->expects($this->once())
            ->method('removeFile')
            ->with(self::REMOTE_FILE);
        $hdfs->expects($this->once())
            ->method('putFile')
            ->with(self::LOCAL_FILE, self::REMOTE_FILE);
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_DIR, self::REMOTE_DIR, true);
    }//function testPutDirWithOverwrites()


    /**
    * Check `putDir()` fails overwriting directory with a file
    *
    * @expectedException \Hdfs\Exception\AlreadyExistsException
    */
    public function testPutDirFailOverwriteDirectoryWithFileFails ()
    {
        $level1 = array(self::LOCAL_FILE);

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')->with(self::LOCAL_DIR)->will($this->returnValue(true));
        $fs->method('isDir')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_FILE, false)
                )
            ));
        $fs->method('isReadable')
            ->will($this->returnValueMap(
                array(
                    array(self::LOCAL_DIR, true),
                    array(self::LOCAL_FILE, true)
                )
            ));
        $fs->method('readDir')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue($level1));
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract', array(), '', true, true, true, array('isExists'));
        $hdfs->expects($this->exactly(2))
            ->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(dirname(self::REMOTE_DIR), true),
                        array(self::REMOTE_DIR, true)
                    )
                )
            );

        //assume self::REMOTE_FILE is a dir
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));
        $hdfs->method('stat')->with(self::REMOTE_FILE)->will($this->returnValue($entry));
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->putDir(self::LOCAL_DIR, self::REMOTE_DIR);
    }//function testPutDirFailOverwriteDirectoryWithFileFails()


    /**
    * Check `getDir()` throws exception if parent directory for local path does not exist
    *
    * @expectedException \Hdfs\Exception\NotFoundException
    */
    public function testGetDirLocalNotFound ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->once())
            ->method('isExists')
            ->with(self::LOCAL_DIR)
            ->will($this->returnValue(false));
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->getDir(self::REMOTE_DIR, self::LOCAL_SUBDIR);
    }//function testGetDirLocalNotFound()


    /**
    * Check `getDir()` throws exception if local path is not a directory
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testGetDirIllegalArgument ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->expects($this->exactly(2))
            ->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(self::LOCAL_DIR, true),
                        array(self::LOCAL_SUBDIR, true)
                    )
                )
            );
        $fs->expects($this->exactly(2))
            ->method('isDir')
            ->will(
                $this->returnCallback(
                    function($path) {
                        return HdfsAbstractTest::LOCAL_DIR == $path;
                    }
                )
            );
        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $hdfs->getDir(self::REMOTE_DIR, self::LOCAL_SUBDIR);
    }//function testGetDirIllegalArgument()


    /**
    * Check `getDir()` downloads hdfs directory correctly when no overwrites occur
    *
    */
    public function testGetDirNoOverwrites ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(dirname(self::LOCAL_DIR), true),
                        array(self::LOCAL_DIR, false),
                        array(self::LOCAL_SUBDIR, false),
                        array(self::LOCAL_FILE, false)
                    )
                )
            );
        $fs->method('isDir')->will($this->returnValue(true));
        $fs->method('isWritable')->will($this->returnValue(true));

        $fs->expects($this->exactly(2))
            ->method('makeDir')
            ->will(
                $this->returnValueMap(
                    array(
                        array(self::LOCAL_DIR, null),
                        array(self::LOCAL_SUBDIR, null)
                    )
                )
            );

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $entrySub = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entrySub->method('isDir')->will($this->returnValue(true));
        $entrySub->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        $entryFile = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryFile->method('isDir')->will($this->returnValue(false));
        $entryFile->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));
        $level1 = array($entrySub, $entryFile);
        $level2 = array();

        $hdfs->expects($this->at(0))
            ->method('readDir')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($level1));
        $hdfs->expects($this->at(1))
            ->method('readDir')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($level2));

        $hdfs->expects($this->at(2))
            ->method('getFile')
            ->with(self::REMOTE_FILE, self::LOCAL_FILE);

        $hdfs->getDir(self::REMOTE_DIR, self::LOCAL_DIR);
    }//function testGetDirNoOverwrites()


    /**
    * Check `getDir()` downloads hdfs directory correctly when some overwrites occur
    *
    */
    public function testGetDirWithOverwrites ()
    {
        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $fs->method('isExists')
            ->will(
                $this->returnValueMap(
                    array(
                        array(dirname(self::LOCAL_DIR), true),
                        array(self::LOCAL_DIR, true),
                        array(self::LOCAL_SUBDIR, false),
                        array(self::LOCAL_FILE, true)
                    )
                )
            );
        $fs->method('isDir')->will($this->returnCallback(function($path) {
            return $path != HdfsAbstractTest::LOCAL_FILE;
        }));
        $fs->method('isWritable')->will($this->returnValue(true));
        $fs->expects($this->once())
            ->method('removeFile')
            ->with(self::LOCAL_FILE);

        $fs->expects($this->once())
            ->method('makeDir')
            ->with(self::LOCAL_SUBDIR);

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->setFilesystemWrapper($fs);

        $entrySub = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entrySub->method('isDir')->will($this->returnValue(true));
        $entrySub->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        $entryFile = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryFile->method('isDir')->will($this->returnValue(false));
        $entryFile->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));
        $level1 = array($entrySub, $entryFile);
        $level2 = array();

        $hdfs->expects($this->at(0))
            ->method('readDir')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($level1));
        $hdfs->expects($this->at(1))
            ->method('readDir')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($level2));

        $hdfs->expects($this->at(2))
            ->method('getFile')
            ->with(self::REMOTE_FILE, self::LOCAL_FILE);

        $hdfs->getDir(self::REMOTE_DIR, self::LOCAL_DIR, true);
    }//function testGetDirWithOverwrites()


    /**
    * Check `copyFile()` throws exception if source path is not a file.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testCopyFileSourceNotAFileException ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entry));

        $hdfs->copyFile(self::REMOTE_SUBDIR, self::REMOTE_FILE2);
    }//function testCopyFileSourceNotAFileException()


    /**
    * Check `copyFile()` throws exception if destination path exists and it is not a file.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testCopyFileDestinationNotAFileException ()
    {
        $entryFile = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryFile->method('isFile')->will($this->returnValue(true));
        $entryDir = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryDir->method('isFile')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entryFile));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entryDir));

        $hdfs->copyFile(self::REMOTE_FILE, self::REMOTE_SUBDIR, true);
    }//function testCopyFileDestinationNotAFileException()


    /**
    * Check `copyFile()` throws exception if destination path already exists.
    *
    * @expectedException \Hdfs\Exception\AlreadyExistsException
    */
    public function testCopyFileDestinationAlreadyExists ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->exactly(2))
            ->method('stat')
            ->will($this->returnValue($entry));

        $hdfs->copyFile(self::REMOTE_FILE, self::REMOTE_SUBDIR, false);
    }//function testCopyFileDestinationAlreadyExists()


    /**
    * Check `copyFile()` with overwriting existing file when no errors occur.
    *
    */
    public function testCopyFileWithOverwrite ()
    {
        $tmpDir = sys_get_temp_dir();
        $tmpFile = "$tmpDir/". md5(self::REMOTE_FILE2);

        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->exactly(2))
            ->method('stat')
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('removeFile')
            ->with(self::REMOTE_FILE2);
        $hdfs->expects($this->once())
            ->method('getFile')
            ->with(self::REMOTE_FILE, $tmpFile);
        $hdfs->expects($this->once())
            ->method('putFile')
            ->with($tmpFile, self::REMOTE_FILE2);

        $hdfs->copyFile(self::REMOTE_FILE, self::REMOTE_FILE2, true);
    }//function testCopyFileWithOverwrite()


    /**
    * Check `copyDir()` throws exception if source path is not a directory.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testCopyDirSourceNotADirectoryException ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));

        $hdfs->copyDir(self::REMOTE_FILE, self::REMOTE_SUBDIR2);
    }//function testCopyDirSourceNotADirectoryException()


    /**
    * Check `copyDir()` throws exception if destination path exists and it is not a file.
    *
    * @expectedException \Hdfs\Exception\IllegalArgumentException
    */
    public function testCopyDirDestinationNotADirectoryException ()
    {
        $entryDir = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryDir->method('isDir')->will($this->returnValue(true));
        $entryFile = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entryFile->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->at(0))
            ->method('stat')
            ->with(self::REMOTE_SUBDIR)
            ->will($this->returnValue($entryDir));
        $hdfs->expects($this->at(1))
            ->method('stat')
            ->with(self::REMOTE_FILE2)
            ->will($this->returnValue($entryFile));

        $hdfs->copyDir(self::REMOTE_SUBDIR, self::REMOTE_FILE2, true);
    }//function testCopyDirDestinationNotADirectoryException()


    /**
    * Check `copyDir()` whith overwriting existing files.
    *
    */
    public function testCopyDirWithOverwrites ()
    {
        $entries = new StdClass();
        $entries->dir = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->dir->method('isDir')->will($this->returnValue(true));
        $entries->dir->method('getName')->will($this->returnValue(basename(self::REMOTE_DIR)));
        $entries->sub = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->sub->method('isDir')->will($this->returnValue(true));
        $entries->sub->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        $entries->file = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->file->method('isDir')->will($this->returnValue(false));
        $entries->file->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));
        $entries->dir2 = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->dir2->method('isDir')->will($this->returnValue(true));
        $entries->dir2->method('getName')->will($this->returnValue(basename(self::REMOTE_DIR2)));
        $entries->sub2 = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->sub2->method('isDir')->will($this->returnValue(true));
        $entries->sub2->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR2)));
        $entries->file2 = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->file2->method('isDir')->will($this->returnValue(false));
        $entries->file2->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE2)));

        $level1 = array($entries->sub, $entries->file);
        $level2 = array();

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->method('stat')
            ->will($this->returnCallback(
                function($path) use ($entries) {
                    switch ($path)
                    {
                        case HdfsAbstractTest::REMOTE_DIR     : return $entries->dir;
                        case HdfsAbstractTest::REMOTE_DIR2    : return $entries->dir2;
                        case HdfsAbstractTest::REMOTE_FILE    : return $entries->file;
                        case HdfsAbstractTest::REMOTE_FILE2   : return $entries->file2;
                        case HdfsAbstractTest::REMOTE_SUBDIR  : return $entries->sub;
                        case HdfsAbstractTest::REMOTE_SUBDIR2 : throw new \Hdfs\Exception\NotFoundException("Path does not exist", false);
                        default : throw new \Exception('Unexpected behavior: '. $path);
                    }
                }
            ));
        $hdfs->method('readDir')
            ->will($this->returnCallback(
                function($path) use ($level1, $level2) {
                    switch ($path)
                    {
                        case HdfsAbstractTest::REMOTE_DIR : return $level1;
                        case HdfsAbstractTest::REMOTE_SUBDIR : return $level2;
                        default : throw new \Exception('Unexpected behavior: '. $path);
                    }
                }
            ));

        $hdfs->expects($this->once())->method('removeFile')->with(self::REMOTE_FILE2);
        $hdfs->expects($this->once())->method('makeDir')->with(self::REMOTE_SUBDIR2);

        $tmpDir = sys_get_temp_dir();
        $tmpFile = "$tmpDir/". md5(self::REMOTE_FILE2);
        $hdfs->expects($this->once())
            ->method('removeFile')
            ->with(self::REMOTE_FILE2);
        $hdfs->expects($this->once())
            ->method('getFile')
            ->with(self::REMOTE_FILE, $tmpFile);
        $hdfs->expects($this->once())
            ->method('putFile')
            ->with($tmpFile, self::REMOTE_FILE2);

        $hdfs->copyDir(self::REMOTE_DIR, self::REMOTE_DIR2, true);
    }//function testCopyDirWithOverwrites()


    /**
    * Check `delete()` invoked on file
    *
    */
    public function testDeleteOnFile ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('removeFile')
            ->with(self::REMOTE_FILE);

        $hdfs->delete(self::REMOTE_FILE);
    }//function testDeleteOnFile()


    /**
    * Check `delete()` invoked on directory
    *
    */
    public function testDeleteOnDir ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(true));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('removeDir')
            ->with(self::REMOTE_DIR);

        $hdfs->delete(self::REMOTE_DIR);
    }//function testDeleteOnDir()


    /**
    * Check `deleteRecursive()` invoked on file
    *
    */
    public function testDeleteRecursiveOnFile ()
    {
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isDir')->will($this->returnValue(false));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));
        $hdfs->expects($this->once())
            ->method('removeFile')
            ->with(self::REMOTE_FILE);

        $hdfs->deleteRecursive(self::REMOTE_FILE);
    }//function testDeleteRecursiveOnFile()


    /**
    * Check `deleteRecursive()` invoked on directory
    *
    */
    public function testDeleteRecursiveOnDirectory ()
    {
        $entries = new StdClass();
        $entries->dir = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->dir->method('isDir')->will($this->returnValue(true));
        $entries->dir->method('getName')->will($this->returnValue(basename(self::REMOTE_DIR)));
        $entries->sub = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->sub->method('isDir')->will($this->returnValue(true));
        $entries->sub->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        $entries->file = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->file->method('isDir')->will($this->returnValue(false));
        $entries->file->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));

        $level1 = array($entries->sub, $entries->file);
        $level2 = array();

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->method('stat')
            ->will($this->returnCallback(
                function($path) use ($entries) {
                    switch ($path)
                    {
                        case HdfsAbstractTest::REMOTE_DIR     : return $entries->dir;
                        case HdfsAbstractTest::REMOTE_FILE    : return $entries->file;
                        case HdfsAbstractTest::REMOTE_SUBDIR  : return $entries->sub;
                        default : throw new \Exception('Unexpected behavior: '. $path);
                    }
                }
            ));
        $hdfs->method('readDir')
            ->will($this->returnCallback(
                function($path) use ($level1, $level2) {
                    switch ($path)
                    {
                        case HdfsAbstractTest::REMOTE_DIR : return $level1;
                        case HdfsAbstractTest::REMOTE_SUBDIR : return $level2;
                        default : throw new \Exception('Unexpected behavior: '. $path);
                    }
                }
            ));

        $hdfs->expects($this->once())
            ->method('removeFile')
            ->with(self::REMOTE_FILE);
        $hdfs->expects($this->exactly(2))
            ->method('removeDir')
            ->will($this->returnCallback(
                function($path) {
                    switch ($path)
                    {
                        case HdfsAbstractTest::REMOTE_DIR : break; //ok
                        case HdfsAbstractTest::REMOTE_SUBDIR : break; //ok
                        default : throw new \Exception('Unexpected behavior: '. $path);
                    }
                }
            ));

        $hdfs->deleteRecursive(self::REMOTE_DIR);
    }//function testDeleteRecursiveOnDirectory()


    /**
    * Check `getSize()` when no errors occur
    *
    */
    public function testGetSizeSuccess ()
    {
        $size = 1024;
        $entry = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entry->method('isFile')->will($this->returnValue(true));
        $entry->method('getSize')->will($this->returnValue($size));

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_FILE)
            ->will($this->returnValue($entry));

        $result = $hdfs->getSize(self::REMOTE_FILE);
        $this->assertEquals($size, $result);
    }//function testGetSizeSuccess()


    /**
    * Check `getSizeRecursive()` when no errors occur
    *
    */
    public function testGetSizeRecursiveSuccess ()
    {
        $fileSize = 1024;
        $entries = new StdClass();
        $entries->dir = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->dir->method('isDir')->will($this->returnValue(true));
        $entries->dir->method('getName')->will($this->returnValue(basename(self::REMOTE_DIR)));
        $entries->sub = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->sub->method('isDir')->will($this->returnValue(true));
        $entries->sub->method('getName')->will($this->returnValue(basename(self::REMOTE_SUBDIR)));
        $entries->file = $this->getMockBuilder('\\Hdfs\\EntryStatus')->disableOriginalConstructor()->getMock();
        $entries->file->method('isDir')->will($this->returnValue(false));
        $entries->file->method('getName')->will($this->returnValue(basename(self::REMOTE_FILE)));
        $entries->file->method('getSize')->will($this->returnValue($fileSize));

        $level1 = array($entries->sub, $entries->file);
        $level2 = array();

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('stat')
            ->with(self::REMOTE_DIR)
            ->will($this->returnValue($entries->dir));
        $hdfs->expects($this->exactly(2))
            ->method('readDir')
            ->will($this->returnCallback(
                function($path) use ($level1, $level2) {
                    switch ($path)
                    {
                        case HdfsAbstractTest::REMOTE_DIR : return $level1;
                        case HdfsAbstractTest::REMOTE_SUBDIR : return $level2;
                        default : throw new \Exception('Unexpected behavior: '. $path);
                    }
                }
            ));

        $result = $hdfs->getSizeRecursive(self::REMOTE_DIR);
        $this->assertEquals($fileSize, $result);
    }//function testGetSizeRecursiveSuccess()


    /**
    * Check `readFile()`
    *
    */
    public function testReadFile ()
    {
        $offset        = 10;
        $length        = 100;
        $tmpFileHandle = 'resource';
        $content       = str_pad('q', $length);

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('getFile')
            ->with(self::REMOTE_FILE, $this->anything());

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $hdfs->setFilesystemWrapper($fs);

        $fs->expects($this->at(0))->method('openFile')->with($this->anything(), 'r')->will($this->returnValue($tmpFileHandle));
        $fs->expects($this->at(1))->method('seek')->with($tmpFileHandle, $offset);
        $fs->expects($this->at(2))->method('read')->with($tmpFileHandle, $length)->will($this->returnValue($content));
        $fs->expects($this->at(3))->method('closeFile')->with($tmpFileHandle);
        $fs->expects($this->at(4))->method('removeFile');

        $result = $hdfs->readFile(self::REMOTE_FILE, $offset, $length);

        $this->assertEquals($content, $result);
    }//function testReadFile()


    /**
    * Check `writeFile()`
    *
    */
    public function testWriteFile ()
    {
        $tmpFileHandle = 'resource';
        $content       = str_pad('q', 100);

        $hdfs = $this->getMockForAbstractClass('\\Hdfs\\HdfsAbstract');
        $hdfs->expects($this->once())
            ->method('putFile')
            ->with($this->anything(), self::REMOTE_FILE);

        $fs = $this->getMock('\\Hdfs\\FilesystemWrapper');
        $hdfs->setFilesystemWrapper($fs);

        $fs->expects($this->at(0))->method('saveFile')->with($this->anything(), $content);
        $fs->expects($this->at(1))->method('removeFile');

        $hdfs->writeFile(self::REMOTE_FILE, $content);
    }//function testWriteFile()

}//class HdfsAbstractTest