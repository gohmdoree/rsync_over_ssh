#!/usr/bin/perl 

### begin comment ##################################
####################################################
#  v 0.1
#  a charles yoo <cyoo@guardiandigital.com>
#
#  d backup script
####################################################
### end comment ####################################

### begin description ##############################
####################################################
#
# howTo()
# 	reports usage of the script
# readConfig()
#	reads/parses $Bin/$BACKUP_CONFIG
# listConfig()
#	lists backups in the $Bin/BACKUP_CONFIG
#	and any associated values
# listClasses()
#	lists backup classes
# runBackup()
#	runs backup
# runKey()
#	runs a specific backup by key
# runClass()
#	runs a group of backups by class
# buildSSH()
#	creates ssh string
# buildCMD() 
# 	creates command string rsync over ssh
# getTimeStamp() 
# 	creates time stamp mm/dd/yyyy hh:mm:ss
#	for use in log files
# getTimeDir()
#	creates time stamp/string for directory
# openLogs()
#	open logs 
# checkDir()
#	checks to see if a directory exists
#	if not, create it
# getOldNew()
#	gets the oldest and newest backups
# removeExpired()
# cleanMe()
#	unlinks and rmdir's oldest directory
# rsyncStats()
#       retrieve output from rsync --stats
#	returns an array with the following:
#	0 - Number of files 
#	1 - Number of files transferred 
# 	2 - Total file size 
#	3 - Total transferred file size 
#	4 - Literal data 
# 	5 - Matched data 
#	6 - File list size
#	7 - Total bytes sent
#	8 - Total bytes received 
#
####################################################
### end description ################################

# use Perl
use FindBin     qw($Bin);
use File::Find  qw(finddepth);
use Getopt::Std qw(getopts);
use DB_File;
use strict;

### start constants
####################################################

# version number
use constant VERSION => '$revision: 0.1 $';

# constants
my %CONFIG 		= ();
my $BACKUP_CONFIG 	= "daily.conf";

# use this constants for easy readability
# will be assigned values from %CONFIG
my $EXCLUDE_OPTS	= "--exclude-from";
my $INCLUDE_OPTS	= "--include-from";
my %RSYNC_VALS		= ();
my $BACKUP_LOG 		= "$Bin/.backup.log";
my $BACKUP_ERR 		= "$Bin/.backup.log.err";
my $BACKUP_LIMIT	= 10;

# directory constants
my $DIR 		= "/home/cyoo/backup/";

# rsync constants
my $RSYNC_OPT   	= "-avz --delete";
my $RSYNC_KEY   	= undef;
my $RSYNC_LINK_OPT  	= "--link-dest=";
my $RSYNC_PWD		= "../";
my $RSYNC_SSH		= undef;

my %opts;
my $CMD = undef;
my @RSTATS = ();
my %RSYNC_STATS;
my $RSYNC_STAT_HASH     = "stats";

####################################################
### end constants

### run script
####################################################

# get command-line arguments
getopts('ab:lc:h:', \%opts);

# read configuration file
&readConfig(\%CONFIG);

if ($opts{'l'}){
	&listHosts(\%CONFIG);
	&listClasses(\%CONFIG);
	&listConfig(\%CONFIG);
}elsif ($opts{'a'}){
	&runConfig(\%CONFIG);
}elsif ($opts{'b'}){
	my $KEY = $opts{'b'};
	# display how-to if no <key>
	&howTo() unless (defined($KEY));
	&runKey($KEY,\%CONFIG);
}elsif ($opts{'c'}){
	my $CLASS = $opts{'c'};
	# display how-to if no <key>
	&howTo() unless (defined($CLASS));
	&runClass($CLASS,\%CONFIG);
}elsif ($opts{'h'}){
	my $HOST = $opts{'h'};
	&howTo() unless (defined($HOST));
	&runHost($HOST,\%CONFIG);
}else{
	&howTo();
}

####################################################
### end script

### start subroutines
####################################################

####################################################

sub howTo(){
	my $VERSION = VERSION;

	print STDERR <<EOF;
rsync over ssh backup script, daily.pl, $VERSION

Usage: $0 { -l | -a | -b <key> | -c <class>}
	-a		run all defined backups
	-b <key>	only run backup specified by <key>
	-c <class>	only run backups specified by <class>
	-h <host>	only run backup specified by <host>
	-l		list all currently defined backups

EOF
	exit -1;
}

####################################################

####################################################
# reads from $Bin/$BACKUP_CONFIG
# in the following format
#
# see: example.conf
# set $COUNT to default as 0 not -1, because 
# funcation setMyVars would not work when a
# value of 0 was passed to it
sub readConfig($){
        my $CFG     = shift || return undef;
        my $COUNT   = 0;
	my $CLASS   = '';

        if (-f "$Bin/$BACKUP_CONFIG"){
                my $IN;

                open CONF, "$Bin/$BACKUP_CONFIG" or
                        die "could not read $Bin/$BACKUP_CONFIG: $!\n";

                while (my $line = <CONF>){
                        chomp $line;
                        next if ($line =~ m/^\#/ || $line eq '');

                        if ($line eq 'config'){
                                $IN = $line;
                                next;
                        }elsif ($line eq 'backup'){
                                $IN = $line;
                                $COUNT++;
                                next;
                        }elsif ($line eq ''){
                                undef $IN;
                        }elsif ($line =~ m/\[desktops\]/){
				$IN = $line;
				$CLASS = "desktops";
				$COUNT = 0;
				next;
                        }elsif ($line =~ m/\[servers\]/){
				$IN = $line;
				$CLASS = "servers";
				$COUNT = 0;
				next;
                        }elsif ($line =~ m/\[customers\]/){
				$IN = $line;
				$CLASS = "customers";
				$COUNT = 0;
				next;
                        }

                        if ($IN eq 'config'){
                                my @T = split /\s+/, $line, 3;
				$RSYNC_KEY = $T[2];
			}elsif ($IN eq 'backup'){
                                my @T = split /\s+/, $line, 3;
                                $CFG->{$CLASS}->{'backup'}[$COUNT]->{$T[1]} = $T[2];
                        }

			# if reach end of class label
			# reset $COUNT
			if ($line =~ m/^\[\/.*\]$/){
			}
                }
                close CONF;
        }else{
                die "$Bin/$BACKUP_CONFIG does not exist!\n";
        }
}

####################################################

sub listClasses($){
	my $CFG   = shift || return undef;
	my $i;

	print "classes: \n";
	for my $k1 (sort keys %$CFG){
		print "$k1 \n";
	}
	
	print "\n\n";
}

####################################################

sub listHosts($){
	my $CFG   = shift || return undef;
	my $i;

	print "hosts: \n";
	for my $k1 (sort keys %$CFG){
		for ($i = 1; $CFG->{$k1}->{'backup'}[$i]; $i++){
			print "$CFG->{$k1}->{'backup'}[$i]->{'host'}\n";
		}
	}

	print "\n\n";
}

####################################################

sub listConfig($){
	my $CFG	  = shift || return undef;
	my $i;

for my $k1 (sort keys %$CFG){
	for ($i = 1; $CFG->{$k1}->{'backup'}[$i]; $i++){
		my $TMP = "[$CFG->{$k1}{'backup'}[$i]->{'key'}] $CFG->{$k1}{'backup'}[$i]->{'name'}";
		print "\n$TMP\n";

		foreach my $FOO (split //, $TMP){
			print '-';
		}

		print "\n";
		print "Host:	    $CFG->{$k1}->{'backup'}[$i]->{'host'}\n";
		print "Source:      $CFG->{$k1}->{'backup'}[$i]->{'src'}\n";
		print "Destination: $CFG->{$k1}->{'backup'}[$i]->{'dest'}\n";
		print "Exclude:     $CFG->{$k1}->{'backup'}[$i]->{'exclude'}\n";
		print "Include:     $CFG->{$k1}->{'backup'}[$i]->{'include'}\n";
		print "\n";
	}
}
}

####################################################
# are not using

sub setMyVars($$$){
	my $CFG 	= shift || return undef;
	my $k1		= shift || return undef;
	my $i		= shift || return undef;

#	 $SOURCE         = $CFG->{$k1}->{'backup'}[$i]->{'src'}; 
#        $DESTINATION    = $CFG->{$k1}->{'backup'}[$i]->{'dest'};
#        $EXCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'exclude'}; 
#        $INCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'include'};
}

####################################################

sub runConfig($){
        my $CFG   = shift || return undef;
	my $i;
	my $TARGET;
	my @pids  = ();
	my $first;
	my $last;
	my $CMD = undef;
	my $COUNT = 0;
	my $STATUS = 0;

for my $k1 (sort keys %$CFG){
        if (my $pid = fork) {                     # parent
#		print "###$k1###$pid###\n";
		waitpid($pid, 0);
        }elsif(defined $pid){
		$COUNT++;
		# open our log
		&openLogs($COUNT);
        	for ($i = 1; $CFG->{$k1}->{'backup'}[$i]; $i++){
			my $HOST	   = $CFG->{$k1}->{'backup'}[$i]->{'host'};
                        my $SOURCE         = $CFG->{$k1}->{'backup'}[$i]->{'src'};
                        my $DESTINATION    = $CFG->{$k1}->{'backup'}[$i]->{'dest'};
                        my $EXCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'exclude'};
                        my $INCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'include'};
                        ($first,$last) = getOldNew($DESTINATION);
                        $CMD = buildCMD($SOURCE, $DESTINATION, $EXCLUDE_FILE, $INCLUDE_FILE, $last);
			print "\n\n$CMD\n\n";
#                        $TARGET        = $CFG->{$k1}->{'backup'}[$i]->{'dest'} . getTimeDir();
                	&runBackup($CMD,$DESTINATION,$HOST);
		}
		waitpid($pid, 0);
		exit 1;
		# let our child go bye bye
		&closeLogs($COUNT);
	}
}
	&conCatLogs($COUNT);

}

####################################################

sub runKey($$){
	my $KEY    = shift || return undef;
	my $CFG	   = shift || return undef;	
	my $i;
	my $TARGET;
	my $CMD;
	my $first;
	my $last;

&openLogs();
for my $k1 (sort keys %$CFG){ 
        for ($i = 1; $CFG->{$k1}->{'backup'}[$i]; $i++){
		if ($CFG->{$k1}{'backup'}[$i]->{'key'} == $KEY){
			my $HOST	   = $CFG->{$k1}->{'backup'}[$i]->{'host'};
                        my $SOURCE         = $CFG->{$k1}->{'backup'}[$i]->{'src'};
                        my $DESTINATION    = $CFG->{$k1}->{'backup'}[$i]->{'dest'};
                        my $EXCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'exclude'};
                        my $INCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'include'};
			($first,$last) = getOldNew($DESTINATION);
	       		$CMD = buildCMD($SOURCE, $DESTINATION, $EXCLUDE_FILE, $INCLUDE_FILE, $last);
#			$TARGET        = $CFG->{$k1}->{'backup'}[$i]->{'dest'} . getTimeDir();
			&runBackup($CMD,$DESTINATION,$HOST);
		}
	}
}
&closeLogs();
}

####################################################

sub runClass($$){
        my $CLASS    = shift || return undef;
        my $CFG      = shift || return undef;
	my $STATE    = 0;
	my $TARGET;
        my $i;
	my $CMD;
	my $first;
	my $last;

	&openLogs();

	for my $k1 (sort keys %$CFG){
		if ($CLASS eq  $k1){
			$STATE = 1;
	        	for ($i = 1; $CFG->{$k1}->{'backup'}[$i]; $i++){
				my $HOST	   = $CFG->{$k1}->{'backup'}[$i]->{'host'};
	                        my $SOURCE         = $CFG->{$k1}->{'backup'}[$i]->{'src'};
        	                my $DESTINATION    = $CFG->{$k1}->{'backup'}[$i]->{'dest'};
                	        my $EXCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'exclude'};
                        	my $INCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'include'};
               			($first,$last) = getOldNew($DESTINATION);
	       			$CMD = buildCMD($SOURCE, $DESTINATION, $EXCLUDE_FILE, $INCLUDE_FILE, $last);
#				$TARGET        = $CFG->{$k1}->{'backup'}[$i]->{'dest'} . getTimeDir();
                       		&runBackup($CMD,$DESTINATION,$HOST);
        		}
		}
	}

	if ($STATE == 0){
		print "invalid class\n";
		exit -1;
	}
	&closeLogs();
}

####################################################

sub runHost($$){
        my $HOST     = shift || return undef;
        my $CFG      = shift || return undef;
        my $STATE    = 0;
	my $TARGET;
        my $i;
	my $CMD;
	my $first;
	my $last;

	&openLogs();
        for my $k1 (sort keys %$CFG){
                        for ($i = 1; $CFG->{$k1}->{'backup'}[$i]; $i++){
				if ($CFG->{$k1}->{'host'}[$i]->{'host'} eq $HOST){
					$STATE 		= 1;
					my $HOST	   = $CFG->{$k1}->{'backup'}[$i]->{'host'};
		                        my $SOURCE         = $CFG->{$k1}->{'backup'}[$i]->{'src'};
        		                my $DESTINATION    = $CFG->{$k1}->{'backup'}[$i]->{'dest'};
                		        my $EXCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'exclude'};
                        		my $INCLUDE_FILE   = $CFG->{$k1}->{'backup'}[$i]->{'include'};
                                	($first,$last) = getOldNew($DESTINATION);
	       				$CMD = buildCMD($SOURCE, $DESTINATION, $EXCLUDE_FILE, $INCLUDE_FILE, $last);
#					$TARGET        = $CFG->{$k1}->{'backup'}[$i]->{'dest'} . getTimeDir();
                                	&runBackup($CMD,$DESTINATION,$HOST);
				}
                        }
        }

        if ($STATE == 0){
                print "invalid host\n";
                exit -1;
        }
	&closeLogs();
}

####################################################

sub runBackup($$$){
	my $CMD	   	= shift || return undef;
	my $DESTINATION = shift || return undef;
	my $HOST	= shift || return undef;

	# open logs
	# &openLogs();
	
	# run command
	# print to write for the log
#	print "\n#################################\n";
#	print "command: " . $CMD . "\n";
#	print "running \@ " .  getTimeStamp() . "\n";
#	print "writing to " . $DESTINATION . "\n\n";

	# parse rsync output
	open FH, "$CMD 2>&1 |";
	foreach my $line (<FH>){
		chomp $line;
		next if ($line eq ''); 
		rsyncStats($line,$DESTINATION,$HOST);
	}
	close FH;

#	system $CMD;
#	print "\nfinished \@ " . getTimeStamp() . "\n";

	# close logs
	# &closeLogs();
}

####################################################

sub buildSSH(){
   	my $RSYNC_SSH = "-e \"ssh -c blowfish -C -i $RSYNC_KEY\"";
	
	return $RSYNC_SSH;
}

####################################################

sub buildCMD($$$$$){
	my $RSYNC_LINK   = "";
	my $INCLUDE      = "";
	my $EXCLUDE      = "";
	my $SOURCE       = shift || return undef;
        my $DESTINATION  = shift || return undef;
	my $EXCLUDE_FILE = shift || return undef;
	my $INCLUDE_FILE = shift || return undef;
	my $LAST	 = shift || return undef;
	my $RSYNC_SSH    = buildSSH();
	my $TODAY	 = getTimeDir();

	# a test to see if this is first time
	# if so, $first will equal 99999999999999

	my $TARGET = $DESTINATION . getTimeDir();
	if ($LAST == 1){
	}else{
		$RSYNC_LINK = "$RSYNC_LINK_OPT\"$DESTINATION$LAST/\"";
	}

#	# if $TARGET and $LAST are the same, we're running again on the same day, remove what is there
#	we don't need this, taken care of by getOldNew()
#	if ($LAST == getTimeDir()){
#		&removeExpired($TARGET);
#	}

	# a test to see if there is an include file & exclude file
        if ($EXCLUDE_FILE eq undef){
	}else{
		$EXCLUDE = "$EXCLUDE_OPTS $EXCLUDE_FILE";
	}

	if ($INCLUDE_FILE eq undef){
	}else{
		$INCLUDE = "$INCLUDE_OPTS $INCLUDE_FILE";
	}	

#	my $CMD = "rsync $RSYNC_OPT $INCLUDE $EXCLUDE $RSYNC_SSH $RSYNC_LINK $SOURCE $DESTINATION" . getTimeDir();
	my $CMD = "rsync --stats $RSYNC_OPT $INCLUDE $EXCLUDE $RSYNC_SSH $RSYNC_LINK $SOURCE $DESTINATION" . getTimeDir();

	return $CMD;
}

####################################################

sub openLogs($){
	my $COUNT = shift || return undef;
	close STDOUT;
	close STDERR;
	if ($COUNT){
		open STDOUT, "> backup$COUNT.log";
		open STDERR, "> backup$COUNT.err.log";
	}else{
		open STDOUT, ">> $BACKUP_LOG";
		open STDERR, ">> $BACKUP_ERR";
	}
}

####################################################

sub conCatLogs($){
	my $COUNT = shift || return undef;

        # close and concatenate our logs to main log
        open LOG, ">> $BACKUP_LOG";
        open ERR, ">> $BACKUP_ERR";

        for (my $i = 0; $i <= $COUNT; $i++){
                open TMP_LOG, "$Bin/backup$COUNT.log";
                open TMP_ERR, "$Bin/backup$COUNT.err.log";
                while (<TMP_LOG>){
                        print LOG $_;
                        print $_;
                }
                while (<TMP_ERR>){
                        print ERR $_;
                        print $_;
                }
                close TMP_LOG;
                close TMP_ERR;
                unlink "$Bin/backup$COUNT.log";
                unlink "$Bin/backup$COUNT.err.log";
        }
}
####################################################

sub closeLogs(){
	close STDOUT;
	close STDERR;
}

####################################################

sub checkDir($){
	my $dir = shift || return undef;

	if (! -e $dir){
		mkdir($dir, 0755) || die "cannot create directory: $dir\n";
	}else{
		# directory already exists
	}
}

####################################################

sub getTimeStamp(){
	my @D = localtime(time);
	$D[4]++;
	$D[5] = $D[5] + 1900;

	for (my $i = 0; $i < 5; $i++){
		if ($D[$i] < 10){
			$D[$i] = "0$D[$i]";
		}
	}
	
	return "$D[4]/$D[3]/$D[5] $D[2]:$D[1]:$D[0]";
}

####################################################

sub getTimeDir(){
        my @D = localtime(time);
        $D[4]++;
        $D[5] = $D[5] + 1900;

        for (my $i = 0; $i < 5; $i++){
                if ($D[$i] < 10){
                       $D[$i] = "0$D[$i]";
                }
        }

        return "$D[5]$D[4]$D[3]";
#        return "$D[5]$D[4]$D[3]$D[2]$D[1]$D[0]";
}

####################################################

# returns the first and last
# * if they are the same, that means we have only one
# directory.  
# * if last equals today, we're forcing a run so
#   we want to unlink/remove it and then 
#   return the second to last one 
sub getOldNew($){
	my $dir         = undef;
	my $first	= 99999999999999;
	my $last        = 1;
	my $second2last = 1;
	my @OLD;
	my $DESTINATION = shift || return undef;
	my $count	= 0;
	my $TODAY	= getTimeDir();

	&checkDir($DESTINATION);

	opendir(DIR, $DESTINATION) || die "can't opendir $DESTINATION: $!";

	while ( my $contents = readdir(DIR) ){
        	if ((-d "$DESTINATION/$contents") && ($contents !~ m/\.+/)){
	                if ($last < $contents){
				$second2last = $last;
	                        $last = $contents;
	                }
	                if ($first > $contents){
	                        $first = $contents;
	        	}
			$count++;
		}else{
	                next;
		}
	}

	close (DIR);
	if ($count >= 10){
		&removeExpired("$DESTINATION$first");
	}

	# if the last dir is today, then we are rerunning 
	if ($last == $TODAY){
		# in the case re rerun, etc.
		# remove/unlink/destroy/rampage/conquer
		&removeExpired("$DESTINATION$last");
		$last = $second2last;
	}
#	does not apply anymore
#	elsif ($first == $last){
#		$first = 1;
#		$last  = 1;
#	}
	
	# if we only had one directory, correct $second2last
	# the logic: from above, if $last is $TODAY, then we
	# are rerunning the script for today, so, we unlink it
	# and set to $second2last
	# in the case the $second2last is 1, then we only have
	# "yesterday's" backup, so set $last to $first
	if ($second2last == 1){
		$last = $first;
	}

	$OLD[0] = $first;
	$OLD[1] = $last;
	return @OLD;
}

####################################################
sub removeExpired($){
	finddepth \&cleanMe, shift;
}

####################################################

sub cleanMe($){
	my $kill = $File::Find::name;
	if (! -l "$kill" && -d "$kill"){
		print "rmdir $kill\n";
		rmdir $kill;
	}else{
		print "unlink $kill\n";
		unlink $kill;
	}
}

####################################################

sub rsyncStats($$){
	my $line 	= shift || return undef;
	my $DESTINATION = shift || return undef;
	my $HOST 	= shift || return undef;
	my %RSYNC_STATS;
	my $RSYNC_HASH_FILE = "$DESTINATION$RSYNC_STAT_HASH" . getTimeDir();

	tie %RSYNC_STATS, 'DB_File', $RSYNC_HASH_FILE, or die; 

	if ($line =~ m/Number\ of\ files\:\ (\d+)/){
		%RSYNC_STATS->{0} =  $1; 
	}
	if ($line =~ m/Number\ of\ files\ transferred\:\ (\d+)/){
		%RSYNC_STATS->{1} =  $1;
	}
	if ($line =~ m/Total\ file\ size\:\ (\d+)/){
		%RSYNC_STATS->{2} = $1;
	}
	if ($line =~ m/Total\ transferred\ file\ size\:\ (\d+)/){
		%RSYNC_STATS->{3} = $1;
	}
	if ($line =~ m/Literal\ data\:\ (\d+)/){
		%RSYNC_STATS->{4} = $1;
	}
	if ($line =~ m/Matched\ data\:\ (\d+)/){
		%RSYNC_STATS->{5} = $1;
	}
	if ($line =~ m/File\ list\ size\:\ (\d+)/){
		%RSYNC_STATS->{6} = $1;
	}
	if ($line =~ m/Total\ bytes\ sent\:\ (\d+)/){
		%RSYNC_STATS->{7} = $1;
	}
	if ($line =~ m/Total\ bytes\ received\:\ (\d+)/){
		%RSYNC_STATS->{8} = $1;
	}
	
	untie %RSYNC_STATS;
#	return @RSTATS;
}

####################################################

####################################################
### end subroutines

