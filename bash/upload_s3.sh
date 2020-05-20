#object-detection-models-training-data-backups/aziz-temp/2018/
echo using: $1

if [[ -z "$1" ]]; then
    echo "Must provide $ 1 in environment" 1>&2
    exit 1
fi

flist=$(AWS_ACCESS_KEY_ID=AKIAV52IIQF223OU4KRG AWS_SECRET_ACCESS_KEY=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 aws s3 ls "s3://$1"  | awk '{print $2}')

arr=($flist)

## now loop through the above array
for i in "${arr[@]}"
do
   echo "AWS_ACCESS_KEY_ID=AKIAV52IIQF223OU4KRG AWS_SECRET_ACCESS_KEY=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 aws s3 cp s3://object-detection-models-training-data-backups/aziz-temp/2018/$i s3://object-detection-models-training-data/2018/$i --recursive"
   eval "AWS_ACCESS_KEY_ID=AKIAV52IIQF223OU4KRG AWS_SECRET_ACCESS_KEY=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 aws s3 cp s3://object-detection-models-training-data-backups/aziz-temp/2018/$i s3://object-detection-models-training-data/2018/$i --recursive"
   #echo $i
   sleep 30
   echo "Done\n"
   # or do whatever with individual element of the array
done