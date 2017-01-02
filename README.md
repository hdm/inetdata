# Internet Data Download

Download and normalize internet data from various sources. This package is normally run on a daily basis (after 10:00am CST).

## Dependencies

### Ubuntu
  * sudo apt-get install coreutils build-essential libssl-dev curl gnupg pigz liblz4-tool

### Ruby

#### Ubuntu 16.04 LTS
 * sudo apt-get install ruby

#### Other Distributions
  * gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3
  * \curl -sSL https://get.rvm.io | bash -s stable --ruby=2.3.3

### inetdata-parsers

The normalization process depends on the tools provided by the inetdata-parsers project. Please see the [README](https://github.com/hdm/inetdata-parsers/) for more information. The inetdata-parsers tools need to be in the system path for the normalization process to complete.

## Configuration

A sample configuration file is provided in ``conf/inetdata.json.sample``. This should be copied to ``conf/inetdata.json`` and updated with your credentials and specific settings for your environment. Credential configuration is described in the *Data Sources* section below. The following system settings are important and should be tuned as needed:

```json
  "storage": "./data/cache/",
  "logs": "./data/logs/",
  "log_stderr": true,
  "log_debug": false,
  "DISABLED_max_ram": "16G",
  "DISABLED_max_cores" : "4",
```
  * *storage*: The storage parameter determines where daily downloads and normalized files are saved. For a typical install, this will consume around 1Tb/mo, but may be more or less depending on what sources are enabled. Keep in mind that the normalizer doesn't delete the source data and search-optimized files (such as MTBLs) can be even larger than the original.

  * *logs*: The logs parameter determines where output from the download and normalize jobs are saved. This requires a minimal amount of storage (300M/mo).

  * *log_stderr*: The log_stderr parameter controls whether or not the download and normalize jobs print to stderr as well as the log. This is useful to enable when running the download or normalize scripts on the command line.

  * *log_debug*: The log_debug parameter controls whether or not the download and normalize jobs log additional output that is helpful for diagnostics.

  * *max_ram*: The max_ram parameter determines how much memory is used for the normalize jobs. The "DISABLED_" prefix should be removed and the value set to approximately half of system memory. The normalizer will not work well on systems with less than 16gb of memory.

  * *max_cores*: The max_cores parameter determines how many threads to use for the normalize jobs. The "DISABLED_" prefix should be removed and the value set to the number of cores that can dedicated to this job. The default is to use all available cores when possible. The "nice" command is used to lower the priority of the normalize job, making this relatively safe to use on shared systems.


## Usage

Once configured and tested a cronjob should be created for bin/daily.sh in the following format. This assumes that the system is in the central time zone (CST). The cronjob should not be run prior to 10:00am CST due to the schedule of common sources. The download and normalization process can take up to 18 hours or longer, especially on slow systems and when larger files are retrieved.
```
10 0 * * * /path/to/inetdata/daily.sh 2>&1 /path/to/inetdata/logs/cronjob.log
```

Download jobs can be run manually through ``bin/download.sh``. To select which sources to download, specify a comma-separated list with the ``-s`` parameter. You can see a list of all enabled sources by running this script with the ``-l`` parameter.

Normalize jobs can be run manually through ``bin/normalize.sh``. To select which sources to normalize, specify a comma-separated list with the ``-s`` parameter. You can see a list of all enabled sources by running this script with the ``-l`` parameter.

## Data Sources

| Name          | Description     | Price |
| ------------- |:-------------:| -----:|
| [Sonar](https://scans.io) | FDNS, RDNS, UDP, TCP, TLS, HTTP, HTTPS scan data  |  FREE |
| [Censys.io](https://www.censys.io/)| TCP, TLS, HTTP, HTTPS scan data    | FREE |
| [CT](https://www.certificate-transparency.org/)| TLS | FREE |
| [CZDS](https://czds.icann.org/) | DNS zone files for "new" global TLDs  | FREE |
| [ARIN](https://www.arin.net) | American IP registry information (ASN, Org, Net, Poc) | FREE |
| [CAIDA PFX2AS IPv4](http://data.caida.org/datasets/routing/routeviews-prefix2as) | Daily snapshots of ASN to IPv4 mappings | FREE |
| [CAIDA PFX2AS IPv6](http://data.caida.org/datasets/routing/routeviews6-prefix2as) | Daily snapshots of ASN to IPv6 mappings | FREE |
| [US Gov](https://raw.githubusercontent.com/GSA/data/gh-pages/dotgov-domains/current-full.csv) | US government domain names | FREE |
| [UK Gov](https://www.gov.uk/government/publications/list-of-gov-uk-domain-names) | UK government domain names | FREE |
| [RIR Delegations](http://ftp.arin.net/pub/stats/) | Regional IP allocations | FREE |
| [PremiumDrops](http://premiumdrops.com/) | DNS zone files for com/net/info/org/biz/xxx/sk/us TLDs  | $24.95/mo |
| [WWWS.io](https://wwws.io/) | Domains across many TLDs (~198m)  |   $9/mo |
| [WhoisXMLAPI.com](https://WhoisXMLAPI.com/)  | New domain whois data  | $109/mo |


### Sonar

Project Sonar is a community project sponsored by Rapid7. The latest data can be found at [https://scans.io/](https://scans.io/). More information about Project Sonar can be found on the offical [website](https://sonar.labs.rapid7.com/).

The download script pulls down the sonar.fdns and sonar.rdns datasets, which are updated weekly. In addition, this project pulls down the sonar.ssl and sonar.moressl "names" files (but not the rest of the certificate data). The normalization process converts the sonar.fdns and sonar.rdns files into a set of
CSVs and MTBLs. These include both a forward and reverse lookup. These normalized files can be queried using standard unix utilities or MTBL front-ends such as mtbl_dump, rmtbl_dump, and mq.


### Censys

The download script pulls down the weekly IPv4 file when configured with credentials. Unfortunately, due to the capped download speed and size of the file, this must be excluded from automatic downloads, since the download process can take more than 24 hours, and the normalize another 8-12 hours. To configure this data
source, register for an account at https://censys.io/ and fill in the following two fields in conf/inetdata.json:

```json
{
  "censys_api_id": "<censys-api-id>",
  "censys_secret": "<censys-secret>",
}
```

Specify the censys source (``-s censys``) to use this with ``bin/download.rb`` or ``bin/normalize.rb``


### Certificate Transparency

The download script pulls down the full CT logs. Unfortunately, due to size of these logs this must be excluded from automatic downloads, since the download process can take more than 12 hours, and the normalize another 5-12 hours. 

Specify the ct source (``-s ct``) to use this with ``bin/download.rb`` or ``bin/normalize.rb``

### CZDS (ICANN)

The download script pulls down all available CZDS zone files. To configure this data source, register for an account at https://czds.icann.org/, then apply for access to all desired zones through the CZDS web portal. The download script will automatically pull down all approved zones for your account. Once an account
has been registered, fill in the czds_token field in conf/inetdata.json:

```json
{
  "czds_token": "<token>",
}
```


### ARIN (Bulk Data)

The download script pulls down the daily nets, pocs, orgs, and asns file from ARIN. This requires the completion of a [bulk access agreement](https://www.arin.net/resources/agreements/bulkwhois.pdf), which needs to be physically mailed and approved by the ARIN team. Although most common use cases can be handled through ARIN REST API, any automation that requires fuzzy matching or an extreme number of queries is better handled through bulk data. Once your account is enabled for bulk data, fill in the arin_api_key field in conf/inetdata.json:


```json
{
  "arin_api_key": "API-<key>",
}
```

### PremiumDrops

The download script pulls down the daily full zone files for following TLDs: com, net, info, org, biz, xxx, sk, and us. This is a commercial service and requires a monthly subscription fee to access. PremiumDrops provides the daily zone at 9:00am CST each day and any cronjob that automates the download should be scheduled after this time. The normalization process converts the zones files into a set of CSVs and MTBLs. These include both a forward and reverse lookup. These normalized files can be queried using standard unix utilities or MTBL front-ends such as mtbl_dump, rmtbl_dump, and mq.

Note that while PremiumDrops supports TLS for most things, the actual data download must be over a clear-text connection.
Once an account has been registered, fill in the followingt credentials in conf/inetdata.json:


```json
{
  "premiumdrops_username": "<email-address>",
  "premiumdrops_password": "<password>",
}
```


### WWWS.IO

The download script pulls down the daily full domain list. This is a commercial service and requires a monthly subscription fee to access. WWWS.IO provides the daily zone at 10:00am CST each day and any cronjob that automates the download should be scheduled after this time. The normalization process sorts the addded, removed, and full list of domains.

Once an account has been registered, fill in the followingt credentials in conf/inetdata.json:


```json
{
  "wwwsio_username": "<email-address>",
  "wwwsio_password": "<password>",
}
```


### WhoisXMLAPI (New Domains)

The download script can pull down whois information for new domains from the WhoisXMLAPI.com service. This is a commercial service and requires a monthly subscription fee to access. WhoisXMLAPI's updates are ready at 10:00am CST each day and any cronjob that automates the download should be scheduled after this time.

To sign up for new domain whois access, visit [https://www.whoisxmlapi.com/new-domain-pricing.php](https://www.whoisxmlapi.com/new-domain-pricing.php). The Enterprise plan ($109/mo) provides whois data for new daily domain registrations. Once an account has been registered, a separate username and password will be emailed to you with download credentials. Add those credentials to conf/inetdata.json:


```json
{
  "whoisxmlapi_username": "<email-address>",
  "whoisxmlapi_password": "<download-password>",
}
```
