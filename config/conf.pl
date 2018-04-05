use strict;
use warnings;
use File::Basename;

print "Helper configuration pg_profile extension\n";
print "\n\n";

sub Help {
    print "Utility con.pl used:\n";
    print "./can.pl COMMAND OPTION1 OPTION2 ... OPTIONn\n";
    print "COMMAND:\n";
    print "help - print help information\n";
    print "version name - create folder name in versions dir - new version for pg_profile extension\n";
	print "build - create exctension folder\n";
}

my $command = $ARGV[0];

if (not defined $command) {
    Help();
    exit;
}

if ($command eq "help") {
    Help();
    exit;
}

if ($command eq "version") {
    Version();
    exit;
}

if ($command eq "build") {
	Build();
	exit;
}

print "Unknown command\n";
Help();

sub Version {
    my $version = $ARGV[1];
    if (not defined $version) {
        print "Error: Not version name\n";
        print "Help: Used format: version name\n";
        print "Used argument help for more information\n";
        exit;
    }

    my $dir_versions = dirname(__FILE__).qq(/)."versions";
    my $file_build_versions = dirname(__FILE__).qq(/)."build.lst";
    if (! -e $dir_versions) {
        mkdir($dir_versions) or die "Can't create $dir_versions: $!\n";
        print "Directory $dir_versions is created\n";
        open my $fbv,">>", $file_build_versions;
        close $fbv;
        print "File $file_build_versions is created\n";
    }
    my $dir_version_name = $dir_versions.q(/).$version;
    if (-e $dir_version_name) {
        print "Version $version is existed\n";
        exit;
    }
    mkdir($dir_version_name) or die "Can't create $dir_version_name: $!\n";
    print "Create direcory of new version: $dir_version_name\n";
    print "Add file name in build.lst file for version\n";

    my $file_build_version_name = $dir_version_name.qq(/)."build.lst";
    open my $fbvn, ">>", $file_build_version_name;
    close $fbvn;
    print "File $file_build_version_name is created\n";
    open my $fbv, ">>", $file_build_versions;
    say $fbv $version;
    close $fbv;
    print "Added string $version to build.lst: $file_build_versions\n";
}

sub Build {
	print "Building ...\n";
    my $file_build_versions = dirname(__FILE__).qq(/)."build.lst";
	my $dir_out_build = dirname(__FILE__).qq(/)."out";
	
    if (! -e $dir_out_build) {
        mkdir($dir_out_build) or die "Can't create $dir_out_build: $!\n";
	}	
	my $file_out_build = $dir_out_build.qq(/)."out.sql";
	open my $fob, '>', $file_out_build or die "Could not open '$file_out_build' $!";
	open (my $fh, "<", $file_build_versions) or die "Could not open file '$file_build_versions' $!";
	while (my $row = <$fh>) {
		chomp $row;
		next if $row eq '';
		open (my $fhsql, "<", $row) or die "Could not open file '$row' $!";
		while (my $str = <$fhsql>) {
			$str = $str =~ s/qq(SET search_path=\@extschema\@,public)//r;
			print $fob $str;
		}
		
		close $fhsql;
	}
	close $fh;
	close $fob;
	
	print "Out file: $file_out_build\n";
}