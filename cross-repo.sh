#!/usr/bin/env bash
# stop script on error
#set -e

sdk_list=(
	"awslabs/aws-c-auth"
	)

test=(
	"awslabs/aws-c-auth"
	"awslabs/aws-c-cal"
	"awslabs/aws-c-compression"
	"awslabs/aws-c-event-stream"
	"awslabs/aws-c-http"
	"awslabs/aws-c-io"
	"awslabs/aws-c-iot"
	"awslabs/aws-c-mqtt"
	"awslabs/aws-c-s3"
	"awslabs/aws-c-sdkutils"
	"awslabs/aws-checksums"
	"awslabs/aws-crt-builder"
	"awslabs/aws-crt-cpp"
	"awslabs/aws-crt-dotnet"
	"awslabs/aws-crt-ffi"
	"awslabs/aws-crt-java"
	"awslabs/aws-crt-nodejs"
	"awslabs/aws-crt-php"
	"awslabs/aws-crt-python"
	"awslabs/aws-crt-ruby"
	"awslabs/aws-crt-swift")

filename=repo_update.csv

# function to open webpage where you can create the PR
function openpr() {
  github_url=`git remote -v | awk '/fetch/{print $2}' | sed -Ee 's#(git@|git://)#https://#' -e 's@com:@com/@' -e 's%\.git$%%' | awk '/github/'`;
  branch_name=`git symbolic-ref HEAD | cut -d"/" -f 3,4`;
  pr_url=$github_url"/compare/main..."$branch_name
  open $pr_url;
}

# enable this to be run as test by not including parameter "create_PR"
if [ $1 == "create_PR" ]; then
	PR=true
	echo "Creating PR's"
else
	PR=false
	echo "Test run. Not creating PR's"
fi

# headers for csv
echo repo, status > $filename

for i in ${!sdk_list[@]}; do

	echo "Repo:" ${sdk_list[$i]}
	# get sdk name
	full_sdk=${sdk_list[$i]}
	IFS='/' read -ra temp <<< "$full_sdk"
	sdk=${temp[1]}
	outputstring=$full_sdk,

	# check if cloned already
	if [ ! -d ${sdk} ]; then
		echo "Cloning https://github.com/${full_sdk}.git"
		git clone --depth 1 --no-checkout "https://github.com/${full_sdk}.git"
		cd "${sdk}"
		
		git sparse-checkout set .github

		# handle master vs main
		if git branch | grep -q "master"; then
			git checkout master
		elif git branch | grep -q "main"; then
			git checkout main
		elif git branch | grep -q "develop"; then
			git checkout develop
		elif git branch | grep -q "version-3"; then
			git checkout version-3
		else 
			echo "No main, master, or develop"
		fi
		cd ".github"
		
		if $PR; then
			git checkout -b "pr-template"
		fi


		# handle - vs _ in file names 
		if [ -f "PULL_REQUEST_TEMPLATE.md" ]; then
			si="PULL_REQUEST_TEMPLATE.md"
		fi
		echo $si
		if [ ! -z ${si} ]; then
			if grep "Issue \#" ${si}; then
				
				#only make PR if this is not a test
				if $PR; then
					# update to new ancient time
					sed -i '' "s/*Issue #, if available:*/*Issue #, and\/or reason for changes (REQUIRED):*/" ${si}

					echo "pushing commit"
					git add ${si}
					git commit -m "change PR template to ask for clearer wording"
					output=$(git push origin "pr-template" 2>&1 | grep "fatal")

					echo "~"
					echo $output
					echo "~"

					if ! grep "fatal" <<< "${output}" ; then
						echo "success pushing commit"
 						openpr
						outputstring+="success pushing commit"
					else
						echo "failed pushing commit:" $output
						outputstring+=$output
					fi
				else
					outputstring+="$(grep -hnr 'Test. Not making a PR')"
					echo "test, not making a pr"
				fi
			else 
				echo "'Issue #' ${sdk} repo"
				outputstring+="'Issue #' ${sdk} repo"
			fi
		else
			echo "PULL_REQUEST_TEMPLATE.md file does not exist"
			outputstring+="PULL_REQUEST_TEMPLATE.md file does not exist: "
			outputstring+=$(ls)
		fi
		cd ../..
		rm -rf $sdk
	else
		echo "${sdk[$i]} already exists"
		outputstring+="${sdk[$i]} already exists"
	fi
	echo $outputstring >> $filename
done
