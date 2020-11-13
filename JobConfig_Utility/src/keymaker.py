#!/usr/bin/env python3

import argparse
import base64
import configparser
import getpass
import logging
import os
import socket
import sys
import urllib.parse
import urllib.request

from urllib.error import URLError, HTTPError
from xml.etree.ElementTree import XML

# Location of our auth provider
IDP_URL = 'https://ssologin.ssogen2.corporate.ge.com/SSOLogin/verify.fcc'

# Location to go after authentication
TARGET_URL = 'https://fssfed.ge.com/fss/idp/startSSO.ping?PartnerSpId=urn:amazon:webservices'


def parse_arguments(args):
    """
    Parse arguments from the command line
    """
    description = ('Acquire an AWS session token via SAML authentication. '
                   'To use a proxy, set the "https_proxy" environment variable. '
                   'If you are on the GE LAN you will also need to exclude internal domains by adding ".ge.com" '
                   'to the "no_proxy" environment variable')

    epilog = ('If any positional arguments are empty strings you will be prompted for them. '
              'If any of the positional arguments are omitted you will be prompted for all arguments.')

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description=description,
                                     epilog=epilog)

    # Required arguments
    parser.add_argument('username', nargs='?', help='Username (SSO) for SAML authentication')
    parser.add_argument('password', nargs='?', help='Password or RSA token for SAML authentication')
    parser.add_argument('role', nargs='?', help='Role name in ACCOUNT:role/ROLE-NAME format')

    # Optional authentication arguments
    parser.add_argument(
        '--mfa',
        action='store_true',
        help='Use hightented multi-factor authentication. Proved your RSA token instead of your SSO password')

    # Profile tuning arguments
    parser.add_argument('--profile', default='default', help='Profile to store the session token as')
    parser.add_argument('--region', default='us-east-1', help='AWS Region to connect to')
    parser.add_argument('--cli-output-format', default='json', help='AWS CLI output format for the profile')
    parser.add_argument('--aws-credential-file',
                        default=os.path.expanduser('~/.aws/credentials'),
                        help='AWS credential file to update')

    # Other application behavioral arguments
    # parser.add_argument('--no-verify-ssl',
    #                     action='store_true',
    #                     help='Disable SSL certificate verification (WARNING: Do not use in production!)')
    parser.add_argument('--debug',
                        action='store_true',
                        help='Enable debug printing (WARNING: Contains sensitive information!)')

    return parser.parse_args(args)


def validate_arguments(args):
    """
    Assert that the arguments have been provided in the required combinations and formats
    """
    if args.mfa:
        global IDP_URL
        global TARGET_URL
        IDP_URL = 'https://ssologin.ssogen2.corporate.ge.com/SSOLogin/gesmaceauth.fcc'
        TARGET_URL = 'https://fssfed.ge.com/fss/idp/startSSO.ping?PartnerSpId=urn:amazon:webservices:mfa'
    # Determine if we need to run in interactive mode
    if args.username is None and args.password is None and args.role is None:
        # If all arguments are missing we can assume interactive mode
        pass
    elif args.username is None or args.password is None or args.role is None:
        print('All positional arguments (username, password, role) should be provided or none should be provided',
              'but a partial set was provided. Unable to guess which is which, exiting')
        raise SystemExit(1)

    return True


def do_sso_auth(sso, password, identity_provider, target_url):
    """
    Perform an SSO authentication against a GE identity_provider endpoint for the target_url
    """
    # Build our request body
    post_body = {
        'username': sso,
        'password': password,
        'target': target_url,
    }
    data = urllib.parse.urlencode(post_body)
    logging.debug('SSO authentication request body: {}'.format(data))
    data = data.encode('ascii')

    # Create a URL opener that handles cookies
    cookie_monster = urllib.request.HTTPCookieProcessor()
    opener = urllib.request.build_opener(cookie_monster)

    # Submit the authentication request
    with opener.open(identity_provider, data) as response:
        the_page = response.read()
        logging.debug('IDP response:\n{}'.format(the_page))
    # Verify the authentication was successful
    for cookie in cookie_monster.cookiejar:
        if cookie.name == 'SMSESSION':
            break
    else:
        if "mfa" in TARGET_URL:
            raise RuntimeError('No SSO session cookie received, provide your RSA token when prompted for a password.')
        
        raise RuntimeError('No SSO session cookie received, check credentials and try again')

    return the_page


def main():
    """
    Main function to handle scripted usage of the module
    """
    # Parse command-line arguments
    args = parse_arguments(sys.argv[1:])

    # Validate that our args are safe to proceed
    validate_arguments(args)

    # Expand home directory alias in the credential file path
    args.aws_credential_file = os.path.expanduser(args.aws_credential_file)

    # Enable debug logging if set by user
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    # Set SSL verification; User prompt is a negative assertion so we apply the inverse
    # session.verify = not args.no_verify_ssl

    # Set proxies for request to AWS STS service
    # First Get local proxy settings via env variables
    proxies = urllib.request.getproxies()

    # Then, Determine if in GE LAN & 
    # Check if https_proxy was set as a local environment variable
    logging.debug('Proxy settings found in environment: {}'.format(proxies))
    try:
        # If in the GE LAN and the https proxy is not set
        if (socket.gethostbyname('sts.amazonaws.com') == '223.255.255.255') and ('https' not in proxies):
            logging.warning("GE LAN detected and proxy missing, adding default proxy information")

            # Set https_proxy
            print('Setting https_proxy to http://am.proxy.ge.com:80/.')
            proxies = {
                'https': 'http://am.proxy.ge.com:80/',
                'no': '.ge.com'
            }
    except OSError as e:
        print(e)

        if e.errno == 8:
            print("The network is not routable. Please fix and try again.")

        raise SystemExit(1)

    # Create a handler & opener with the proxies set above
    proxy_handler = urllib.request.ProxyHandler(proxies)
    opener = urllib.request.build_opener(proxy_handler)
    
    # Prompt for missing SAML credentials
    while not args.username:
        args.username = input('Username: ')
    while not args.password:
        args.password = getpass.getpass('Password: ')

    # Perform SAML authentication
    response_text = do_sso_auth(args.username, args.password, IDP_URL, TARGET_URL).decode('utf8')
    logging.debug('Decoded IDP response: {}'.format(response_text))

    # Extract the SAML assertion
    try:
        saml_xml = XML(response_text)
        assertion = saml_xml.find(".//*[@name='SAMLResponse']").attrib['value']
        logging.debug('Decoded assertion:\n{}'.format(base64.b64decode(assertion)))
    except:
        # TODO: Improve error checking/handling
        print('Response did not contain a valid SAML assertion')
        raise SystemExit(1)

    # Parse the returned assertion and extract the authorized roles
    aws_roles = []
    assertion_xml = XML(base64.b64decode(assertion))

    for saml2attributevalue in assertion_xml.findall(".//*[@Name='https://aws.amazon.com/SAML/Attributes/Role']/"):
        logging.debug(saml2attributevalue.text)
        aws_role = {}
        aws_role['role_arn'], aws_role['principal_arn'] = saml2attributevalue.text.split(',')
        aws_role['name'] = aws_role['role_arn'].split('::')[1]
        aws_roles.append(aws_role)
    logging.debug(aws_roles)

    # If we're in interactive mode, have the user select the role from a list of available roles
    while not args.role:
        print('\nPlease choose the AWS account role you would like to assume:')
        for index, aws_role in enumerate(aws_roles, 1):
            print('[{}]: {}'.format(index, aws_role['name']))
        selection = input('Selection: ')

        if selection.isdigit():
            if 1 > int(selection) > len(aws_roles):
                print('\nInvalid Selection\n')
                continue
            else:
                args.role = aws_roles[int(selection) - 1]['name']
        else:
            print('\nSelection must be an integer')

    # Find the role specified by the user
    found_roles = [r for r in aws_roles if r['name'] == args.role]
    if len(found_roles) != 1:
        print('Role "{}" not found. Run program in interactive mode for a list of available roles.'.format(args.role))
        raise SystemExit(1)
    aws_role = found_roles[0]

    # Use the SAML assertion to get an AWS token from STS
    sts_request = {
        'Action': 'AssumeRoleWithSAML',
        'Version': '2011-06-15',
        'RoleArn': aws_role['role_arn'],
        'PrincipalArn': aws_role['principal_arn'],
        'SAMLAssertion': assertion,
    }
    sts_request_data = urllib.parse.urlencode(sts_request)
    logging.debug('STS Authentication request body: {}'.format(sts_request_data))
    sts_request_data = sts_request_data.encode('ascii')

    # Submit the authentication request
    try:
        with opener.open('https://sts.amazonaws.com/', sts_request_data) as response:
            sts_response = response.read().decode('utf8')
    except HTTPError as e:
        print(e)
        if e.code == 403:
            print('Try using the --mfa flag and provide your RSA token when prompted for a password.')
        else:
            print('Failed to assume role with SAML assertion')
        raise SystemExit(1)
    logging.debug('AWS STS Response: {}'.format(sts_response))

    # Parse the STS response
    sts_response = XML(sts_response)

    # Read in the existing config file
    config = configparser.RawConfigParser()
    config.read(args.aws_credential_file)

    # Create the requested profile if it doesn't exist
    if not config.has_section(args.profile):
        config.add_section(args.profile)

    # Update the profile
    config.set(args.profile, 'output', args.cli_output_format)
    config.set(args.profile, 'region', args.region)
    config.set(args.profile, 'aws_access_key_id',
               sts_response.findtext('.//{https://sts.amazonaws.com/doc/2011-06-15/}AccessKeyId'))
    config.set(args.profile, 'aws_secret_access_key',
               sts_response.findtext('.//{https://sts.amazonaws.com/doc/2011-06-15/}SecretAccessKey'))
    config.set(args.profile, 'aws_session_token',
               sts_response.findtext('.//{https://sts.amazonaws.com/doc/2011-06-15/}SessionToken'))

    # Write the updated config file
    try:
        configfile = open(args.aws_credential_file, 'w+')
    except FileNotFoundError:
        os.makedirs(os.path.dirname(args.aws_credential_file))
        configfile = open(args.aws_credential_file, 'w+')
    config.write(configfile)
    configfile.close()


if __name__ == '__main__':
    main()