# AWS RDS Service Broker [![Build Status](https://travis-ci.org/alphagov/paas-rds-broker.png)](https://travis-ci.org/alphagov/paas-rds-broker)

This is an **experimental** [Cloud Foundry Service Broker](https://docs.cloudfoundry.org/services/overview.html) for [Amazon Relational Database Service (RDS)](https://aws.amazon.com/rds/) supporting [Aurora](https://aws.amazon.com/rds/aurora/), [MariaDB](https://aws.amazon.com/rds/mariadb/), [MySQL](https://aws.amazon.com/rds/mysql/) and [PostgreSQL](https://aws.amazon.com/rds/postgresql/) RDS Databases.

More details can be found at this [Pivotal P.O.V Blog post](http://blog.pivotal.io/pivotal-cloud-foundry/products/a-look-at-cloud-foundrys-service-broker-updates).

## Disclaimer

This is **NOT** presently a production ready Service Broker. This is a work in progress. It is suitable for experimentation and may not become supported in the future.

## Installation

### Locally

Using the standard `go install` (you must have [Go](https://golang.org/) already installed in your local machine):

```
$ go install github.com/alphagov/paas-rds-broker
$ rds-broker -port=3000 -config=<path-to-your-config-file>
```

### Cloud Foundry

The broker can be deployed to an already existing [Cloud Foundry](https://www.cloudfoundry.org/) installation:

```
$ git clone https://github.com/alphagov/paas-rds-broker.git
$ cd rds-broker
```

Modify the [included manifest file](https://github.com/alphagov/paas-rds-broker/blob/master/manifest.yml) to include your AWS credentials and optionally the [sample configuration file](https://github.com/alphagov/paas-rds-broker/blob/master/config-sample.json). Then you can push the broker to your [Cloud Foundry](https://www.cloudfoundry.org/) environment:

```
$ cp config-sample.json config.json
$ cf push rds-broker
```

### Docker

If you want to run the AWS RDS Service Broker on a Docker container, you can use the [cfplatformeng/rds-broker](https://registry.hub.docker.com/u/cfplatformeng/rds-broker/) Docker image.

```
$ docker run -d --name rds-broker -p 3000:3000 \
  -e AWS_ACCESS_KEY_ID=<your-aws-access-key-id> \
  -e AWS_SECRET_ACCESS_KEY=<your-aws-secret-access-key> \
  cfplatformeng/rds-broker
```

The Docker image cames with an [embedded sample configuration file](https://github.com/alphagov/paas-rds-broker/blob/master/config-sample.json). If you want to override it, you can create the Docker image with you custom configuration file by running:

```
$ git clone https://github.com/alphagov/paas-rds-broker.git
$ cd rds-broker
$ bin/build-docker-image
```

### BOSH

This broker can be deployed using the [AWS Service Broker BOSH Release](https://github.com/cf-platform-eng/aws-broker-boshrelease).

## Configuration

Refer to the [Configuration](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md) instructions for details about configuring this broker.

This broker gets the AWS credentials from the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. It requires a user with some [IAM](https://aws.amazon.com/iam/) & [RDS](https://aws.amazon.com/rds/) permissions. Refer to the [iam_policy.json](https://github.com/alphagov/paas-rds-broker/blob/master/iam_policy.json) file to check what actions the user must be allowed to perform.

## Usage

### Managing Service Broker

Configure and deploy the broker using one of the above methods. Then:

1. Check that your Cloud Foundry installation supports [Service Broker API Version v2.6 or greater](https://docs.cloudfoundry.org/services/api.html#changelog)
2. [Register the broker](https://docs.cloudfoundry.org/services/managing-service-brokers.html#register-broker) within your Cloud Foundry installation;
3. [Make Services and Plans public](https://docs.cloudfoundry.org/services/access-control.html#enable-access);
4. Depending on your Cloud Foundry settings, you migh also need to create/bind an [Application Security Group](https://docs.cloudfoundry.org/adminguide/app-sec-groups.html) to allow access to the RDS DB Instances.

### Integrating Service Instances with Applications

Application Developers can start to consume the services using the standard [CF CLI commands](https://docs.cloudfoundry.org/devguide/services/managing-services.html).

Depending on the [broker configuration](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#rds-broker-configuration), Application Depevelopers can send arbitrary parameters on certain broker calls:

#### Provision

Provision calls support the following optional [arbitrary parameters](https://docs.cloudfoundry.org/devguide/services/managing-services.html#arbitrary-params-create):

| Option                       | Type    | Description
|:-----------------------------|:------- |:-----------
| backup_retention_period      | Integer | The number of days that Amazon RDS should retain automatic backups of the DB instance (between `0` and `35`) (*)
| character_set_name           | String  | For supported engines, indicates that the DB instance should be associated with the specified CharacterSet (*)
| dbname                       | String  | The name of the Database to be provisioned. If it does not exists, the broker will create it, otherwise, it will reuse the existing one. If this parameter is not set, the broker will use a random Database name
| preferred_backup_window      | String  | The daily time range during which automated backups are created if automated backups are enabled (*)
| preferred_maintenance_window | String  | The weekly time range during which system maintenance can occur (*)

(*) Refer to the [Amazon Relational Database Service Documentation](https://aws.amazon.com/documentation/rds/) for more details about how to set these properties

#### Update

Update calls support the following optional [arbitrary parameters](https://docs.cloudfoundry.org/devguide/services/managing-services.html#arbitrary-params-update):

| Option                       | Type    | Description
|:-----------------------------|:------- |:-----------
| apply_immediately            | Boolean | Specifies whether the modifications in this request and any pending modifications are asynchronously applied as soon as possible, regardless of the Preferred Maintenance Window setting for the DB instance (*)
| backup_retention_period      | Integer | The number of days that Amazon RDS should retain automatic backups of the DB instance (between `0` and `35`) (*)
| preferred_backup_window      | String  | The daily time range during which automated backups are created if automated backups are enabled (*)
| preferred_maintenance_window | String  | The weekly time range during which system maintenance can occur (*)

(*) Refer to the [Amazon Relational Database Service Documentation](https://aws.amazon.com/documentation/rds/) for more details about how to set these properties

#### Bind

Bind calls support the following optional [arbitrary parameters](https://docs.cloudfoundry.org/devguide/services/application-binding.html#arbitrary-params-binding):

| Option | Type   | Description
|:-------|:------ |:-----------
| dbname | String | The name of the Database to bind the application to (it must be provisioned previously)

## Contributing

In the spirit of [free software](http://www.fsf.org/licensing/essays/free-sw.html), **everyone** is encouraged to help improve this project.

Here are some ways *you* can contribute:

* by using alpha, beta, and prerelease versions
* by reporting bugs
* by suggesting new features
* by writing or editing documentation
* by writing specifications
* by writing code (**no patch is too small**: fix typos, add comments, clean up inconsistent whitespace)
* by refactoring code
* by closing [issues](https://github.com/alphagov/paas-rds-broker/issues)
* by reviewing patches

### Submitting an Issue

We use the [GitHub issue tracker](https://github.com/alphagov/paas-rds-broker/issues) to track bugs and features. Before submitting a bug report or feature request, check to make sure it hasn't already been submitted. You can indicate support for an existing issue by voting it up. When submitting a bug report, please include a [Gist](http://gist.github.com/) that includes a stack trace and any details that may be necessary to reproduce the bug, including your Golang version and operating system. Ideally, a bug report should include a pull request with failing specs.

### Submitting a Pull Request

1. Fork the project.
2. Create a topic branch.
3. Implement your feature or bug fix.
4. Commit and push your changes.
5. Submit a pull request.

## Copyright

Copyright (c) 2015 Pivotal Software Inc. See [LICENSE](https://github.com/alphagov/paas-rds-broker/blob/master/LICENSE) for details.
