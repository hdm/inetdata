# Internet Data Download

Download internet data from various sources

## Dependencies

### Ubuntu
  * sudo apt-get install coreutils build-essential libssl-dev curl gnupg pigz 

### Ruby

#### Ubuntu 16.04 LTS
 * sudo apt-get install ruby

#### Other Distributions
  * gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3
  * \curl -sSL https://get.rvm.io | bash -s stable --ruby=2.3.3


## Configuration
  * conf/inetdata.json
  * conf/domains.txt
  * conf/networks.txt

## Usage
  * bin/daily.sh

## Output
  * data/reports/
  * data/cache/
  * data/log/

## Workflow
  * Update new domains
    * premiumdrops.com ($)
    * czds.icann.org
    * wwws.io ($)
  * Update Whois for New Domains 
    * whoisxmlapi.com ($)
  * Update Sonar FDNS, RDNS, SSL
    * sonar.fdns 
    * sonar.rdns 
    * sonar.ssl 
    * sonar.moressl 
  * Update RIR allocations
    * arin 
    * ripe
    * apnic
    * lacnic
    * afrnic
  * Update BGP prefixes
    * CAIDA IPv4
    * CAIDA IPv6
  * Update Government Domains
    * US
    * UK

## Usage

  * Copy conf/inetdata.json.sample to conf/inetdata.json
  * Open conf/inetdata.json in an editor, configure credentials as needed
  * Run bin/download.rb
  * Run bin/normalize.rb



