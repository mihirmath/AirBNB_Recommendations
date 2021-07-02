import sys
from typing import Any, List, Optional, Union, overload

import boto3
import boto3.utils
import botocore.session
from boto3.exceptions import ResourceNotExistsError, UnknownAPIVersionError
from boto3.resources.factory import ResourceFactory
from botocore.client import Config
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import DataNotFoundError, UnknownServiceError
from botocore.loaders import Loader
from botocore.model import ServiceModel
from mypy_boto3_accessanalyzer.client import AccessAnalyzerClient
from mypy_boto3_acm.client import ACMClient
from mypy_boto3_acm_pca.client import ACMPCAClient
from mypy_boto3_alexaforbusiness.client import AlexaForBusinessClient
from mypy_boto3_amp.client import PrometheusServiceClient
from mypy_boto3_amplify.client import AmplifyClient
from mypy_boto3_amplifybackend.client import AmplifyBackendClient
from mypy_boto3_apigateway.client import APIGatewayClient
from mypy_boto3_apigatewaymanagementapi.client import ApiGatewayManagementApiClient
from mypy_boto3_apigatewayv2.client import ApiGatewayV2Client
from mypy_boto3_appconfig.client import AppConfigClient
from mypy_boto3_appflow.client import AppflowClient
from mypy_boto3_appintegrations.client import AppIntegrationsServiceClient
from mypy_boto3_application_autoscaling.client import ApplicationAutoScalingClient
from mypy_boto3_application_insights.client import ApplicationInsightsClient
from mypy_boto3_appmesh.client import AppMeshClient
from mypy_boto3_appstream.client import AppStreamClient
from mypy_boto3_appsync.client import AppSyncClient
from mypy_boto3_athena.client import AthenaClient
from mypy_boto3_auditmanager.client import AuditManagerClient
from mypy_boto3_autoscaling.client import AutoScalingClient
from mypy_boto3_autoscaling_plans.client import AutoScalingPlansClient
from mypy_boto3_backup.client import BackupClient
from mypy_boto3_batch.client import BatchClient
from mypy_boto3_braket.client import BraketClient
from mypy_boto3_budgets.client import BudgetsClient
from mypy_boto3_ce.client import CostExplorerClient
from mypy_boto3_chime.client import ChimeClient
from mypy_boto3_cloud9.client import Cloud9Client
from mypy_boto3_clouddirectory.client import CloudDirectoryClient
from mypy_boto3_cloudformation.client import CloudFormationClient
from mypy_boto3_cloudformation.service_resource import CloudFormationServiceResource
from mypy_boto3_cloudfront.client import CloudFrontClient
from mypy_boto3_cloudhsm.client import CloudHSMClient
from mypy_boto3_cloudhsmv2.client import CloudHSMV2Client
from mypy_boto3_cloudsearch.client import CloudSearchClient
from mypy_boto3_cloudsearchdomain.client import CloudSearchDomainClient
from mypy_boto3_cloudtrail.client import CloudTrailClient
from mypy_boto3_cloudwatch.client import CloudWatchClient
from mypy_boto3_cloudwatch.service_resource import CloudWatchServiceResource
from mypy_boto3_codeartifact.client import CodeArtifactClient
from mypy_boto3_codebuild.client import CodeBuildClient
from mypy_boto3_codecommit.client import CodeCommitClient
from mypy_boto3_codedeploy.client import CodeDeployClient
from mypy_boto3_codeguru_reviewer.client import CodeGuruReviewerClient
from mypy_boto3_codeguruprofiler.client import CodeGuruProfilerClient
from mypy_boto3_codepipeline.client import CodePipelineClient
from mypy_boto3_codestar.client import CodeStarClient
from mypy_boto3_codestar_connections.client import CodeStarconnectionsClient
from mypy_boto3_codestar_notifications.client import CodeStarNotificationsClient
from mypy_boto3_cognito_identity.client import CognitoIdentityClient
from mypy_boto3_cognito_idp.client import CognitoIdentityProviderClient
from mypy_boto3_cognito_sync.client import CognitoSyncClient
from mypy_boto3_comprehend.client import ComprehendClient
from mypy_boto3_comprehendmedical.client import ComprehendMedicalClient
from mypy_boto3_compute_optimizer.client import ComputeOptimizerClient
from mypy_boto3_config.client import ConfigServiceClient
from mypy_boto3_connect.client import ConnectClient
from mypy_boto3_connect_contact_lens.client import ConnectContactLensClient
from mypy_boto3_connectparticipant.client import ConnectParticipantClient
from mypy_boto3_cur.client import CostandUsageReportServiceClient
from mypy_boto3_customer_profiles.client import CustomerProfilesClient
from mypy_boto3_databrew.client import GlueDataBrewClient
from mypy_boto3_dataexchange.client import DataExchangeClient
from mypy_boto3_datapipeline.client import DataPipelineClient
from mypy_boto3_datasync.client import DataSyncClient
from mypy_boto3_dax.client import DAXClient
from mypy_boto3_detective.client import DetectiveClient
from mypy_boto3_devicefarm.client import DeviceFarmClient
from mypy_boto3_devops_guru.client import DevopsGuruClient
from mypy_boto3_directconnect.client import DirectConnectClient
from mypy_boto3_discovery.client import ApplicationDiscoveryServiceClient
from mypy_boto3_dlm.client import DLMClient
from mypy_boto3_dms.client import DatabaseMigrationServiceClient
from mypy_boto3_docdb.client import DocDBClient
from mypy_boto3_ds.client import DirectoryServiceClient
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
from mypy_boto3_dynamodbstreams.client import DynamoDBStreamsClient
from mypy_boto3_ebs.client import EBSClient
from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.service_resource import EC2ServiceResource
from mypy_boto3_ec2_instance_connect.client import EC2InstanceConnectClient
from mypy_boto3_ecr.client import ECRClient
from mypy_boto3_ecr_public.client import ECRPublicClient
from mypy_boto3_ecs.client import ECSClient
from mypy_boto3_efs.client import EFSClient
from mypy_boto3_eks.client import EKSClient
from mypy_boto3_elastic_inference.client import ElasticInferenceClient
from mypy_boto3_elasticache.client import ElastiCacheClient
from mypy_boto3_elasticbeanstalk.client import ElasticBeanstalkClient
from mypy_boto3_elastictranscoder.client import ElasticTranscoderClient
from mypy_boto3_elb.client import ElasticLoadBalancingClient
from mypy_boto3_elbv2.client import ElasticLoadBalancingv2Client
from mypy_boto3_emr.client import EMRClient
from mypy_boto3_emr_containers.client import EMRContainersClient
from mypy_boto3_es.client import ElasticsearchServiceClient
from mypy_boto3_events.client import EventBridgeClient
from mypy_boto3_firehose.client import FirehoseClient
from mypy_boto3_fms.client import FMSClient
from mypy_boto3_forecast.client import ForecastServiceClient
from mypy_boto3_forecastquery.client import ForecastQueryServiceClient
from mypy_boto3_frauddetector.client import FraudDetectorClient
from mypy_boto3_fsx.client import FSxClient
from mypy_boto3_gamelift.client import GameLiftClient
from mypy_boto3_glacier.client import GlacierClient
from mypy_boto3_glacier.service_resource import GlacierServiceResource
from mypy_boto3_globalaccelerator.client import GlobalAcceleratorClient
from mypy_boto3_glue.client import GlueClient
from mypy_boto3_greengrass.client import GreengrassClient
from mypy_boto3_greengrassv2.client import GreengrassV2Client
from mypy_boto3_groundstation.client import GroundStationClient
from mypy_boto3_guardduty.client import GuardDutyClient
from mypy_boto3_health.client import HealthClient
from mypy_boto3_healthlake.client import HealthLakeClient
from mypy_boto3_honeycode.client import HoneycodeClient
from mypy_boto3_iam.client import IAMClient
from mypy_boto3_iam.service_resource import IAMServiceResource
from mypy_boto3_identitystore.client import IdentityStoreClient
from mypy_boto3_imagebuilder.client import ImagebuilderClient
from mypy_boto3_importexport.client import ImportExportClient
from mypy_boto3_inspector.client import InspectorClient
from mypy_boto3_iot1click_devices.client import IoT1ClickDevicesServiceClient
from mypy_boto3_iot1click_projects.client import IoT1ClickProjectsClient
from mypy_boto3_iot.client import IoTClient
from mypy_boto3_iot_data.client import IoTDataPlaneClient
from mypy_boto3_iot_jobs_data.client import IoTJobsDataPlaneClient
from mypy_boto3_iotanalytics.client import IoTAnalyticsClient
from mypy_boto3_iotdeviceadvisor.client import IoTDeviceAdvisorClient
from mypy_boto3_iotevents.client import IoTEventsClient
from mypy_boto3_iotevents_data.client import IoTEventsDataClient
from mypy_boto3_iotfleethub.client import IoTFleetHubClient
from mypy_boto3_iotsecuretunneling.client import IoTSecureTunnelingClient
from mypy_boto3_iotsitewise.client import IoTSiteWiseClient
from mypy_boto3_iotthingsgraph.client import IoTThingsGraphClient
from mypy_boto3_iotwireless.client import IoTWirelessClient
from mypy_boto3_ivs.client import IVSClient
from mypy_boto3_kafka.client import KafkaClient
from mypy_boto3_kendra.client import KendraClient
from mypy_boto3_kinesis.client import KinesisClient
from mypy_boto3_kinesis_video_archived_media.client import KinesisVideoArchivedMediaClient
from mypy_boto3_kinesis_video_media.client import KinesisVideoMediaClient
from mypy_boto3_kinesis_video_signaling.client import KinesisVideoSignalingChannelsClient
from mypy_boto3_kinesisanalytics.client import KinesisAnalyticsClient
from mypy_boto3_kinesisanalyticsv2.client import KinesisAnalyticsV2Client
from mypy_boto3_kinesisvideo.client import KinesisVideoClient
from mypy_boto3_kms.client import KMSClient
from mypy_boto3_lakeformation.client import LakeFormationClient
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_lex_models.client import LexModelBuildingServiceClient
from mypy_boto3_lex_runtime.client import LexRuntimeServiceClient
from mypy_boto3_license_manager.client import LicenseManagerClient
from mypy_boto3_lightsail.client import LightsailClient
from mypy_boto3_location.client import LocationServiceClient
from mypy_boto3_logs.client import CloudWatchLogsClient
from mypy_boto3_lookoutvision.client import LookoutforVisionClient
from mypy_boto3_machinelearning.client import MachineLearningClient
from mypy_boto3_macie2.client import Macie2Client
from mypy_boto3_macie.client import MacieClient
from mypy_boto3_managedblockchain.client import ManagedBlockchainClient
from mypy_boto3_marketplace_catalog.client import MarketplaceCatalogClient
from mypy_boto3_marketplace_entitlement.client import MarketplaceEntitlementServiceClient
from mypy_boto3_marketplacecommerceanalytics.client import MarketplaceCommerceAnalyticsClient
from mypy_boto3_mediaconnect.client import MediaConnectClient
from mypy_boto3_mediaconvert.client import MediaConvertClient
from mypy_boto3_medialive.client import MediaLiveClient
from mypy_boto3_mediapackage.client import MediaPackageClient
from mypy_boto3_mediapackage_vod.client import MediaPackageVodClient
from mypy_boto3_mediastore.client import MediaStoreClient
from mypy_boto3_mediastore_data.client import MediaStoreDataClient
from mypy_boto3_mediatailor.client import MediaTailorClient
from mypy_boto3_meteringmarketplace.client import MarketplaceMeteringClient
from mypy_boto3_mgh.client import MigrationHubClient
from mypy_boto3_migrationhub_config.client import MigrationHubConfigClient
from mypy_boto3_mobile.client import MobileClient
from mypy_boto3_mq.client import MQClient
from mypy_boto3_mturk.client import MTurkClient
from mypy_boto3_mwaa.client import MWAAClient
from mypy_boto3_neptune.client import NeptuneClient
from mypy_boto3_network_firewall.client import NetworkFirewallClient
from mypy_boto3_networkmanager.client import NetworkManagerClient
from mypy_boto3_opsworks.client import OpsWorksClient
from mypy_boto3_opsworks.service_resource import OpsWorksServiceResource
from mypy_boto3_opsworkscm.client import OpsWorksCMClient
from mypy_boto3_organizations.client import OrganizationsClient
from mypy_boto3_outposts.client import OutpostsClient
from mypy_boto3_personalize.client import PersonalizeClient
from mypy_boto3_personalize_events.client import PersonalizeEventsClient
from mypy_boto3_personalize_runtime.client import PersonalizeRuntimeClient
from mypy_boto3_pi.client import PIClient
from mypy_boto3_pinpoint.client import PinpointClient
from mypy_boto3_pinpoint_email.client import PinpointEmailClient
from mypy_boto3_pinpoint_sms_voice.client import PinpointSMSVoiceClient
from mypy_boto3_polly.client import PollyClient
from mypy_boto3_pricing.client import PricingClient
from mypy_boto3_qldb.client import QLDBClient
from mypy_boto3_qldb_session.client import QLDBSessionClient
from mypy_boto3_quicksight.client import QuickSightClient
from mypy_boto3_ram.client import RAMClient
from mypy_boto3_rds.client import RDSClient
from mypy_boto3_rds_data.client import RDSDataServiceClient
from mypy_boto3_redshift.client import RedshiftClient
from mypy_boto3_redshift_data.client import RedshiftDataAPIServiceClient
from mypy_boto3_rekognition.client import RekognitionClient
from mypy_boto3_resource_groups.client import ResourceGroupsClient
from mypy_boto3_resourcegroupstaggingapi.client import ResourceGroupsTaggingAPIClient
from mypy_boto3_robomaker.client import RoboMakerClient
from mypy_boto3_route53.client import Route53Client
from mypy_boto3_route53domains.client import Route53DomainsClient
from mypy_boto3_route53resolver.client import Route53ResolverClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import S3ServiceResource
from mypy_boto3_s3control.client import S3ControlClient
from mypy_boto3_s3outposts.client import S3OutpostsClient
from mypy_boto3_sagemaker.client import SageMakerClient
from mypy_boto3_sagemaker_a2i_runtime.client import AugmentedAIRuntimeClient
from mypy_boto3_sagemaker_edge.client import SagemakerEdgeManagerClient
from mypy_boto3_sagemaker_featurestore_runtime.client import SagemakerFeatureStoreRuntimeClient
from mypy_boto3_sagemaker_runtime.client import SageMakerRuntimeClient
from mypy_boto3_savingsplans.client import SavingsPlansClient
from mypy_boto3_schemas.client import SchemasClient
from mypy_boto3_sdb.client import SimpleDBClient
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from mypy_boto3_securityhub.client import SecurityHubClient
from mypy_boto3_serverlessrepo.client import ServerlessApplicationRepositoryClient
from mypy_boto3_service_quotas.client import ServiceQuotasClient
from mypy_boto3_servicecatalog.client import ServiceCatalogClient
from mypy_boto3_servicecatalog_appregistry.client import AppRegistryClient
from mypy_boto3_servicediscovery.client import ServiceDiscoveryClient
from mypy_boto3_ses.client import SESClient
from mypy_boto3_sesv2.client import SESV2Client
from mypy_boto3_shield.client import ShieldClient
from mypy_boto3_signer.client import SignerClient
from mypy_boto3_sms.client import SMSClient
from mypy_boto3_sms_voice.client import SMSVoiceClient
from mypy_boto3_snowball.client import SnowballClient
from mypy_boto3_sns.client import SNSClient
from mypy_boto3_sns.service_resource import SNSServiceResource
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_sqs.service_resource import SQSServiceResource
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_sso.client import SSOClient
from mypy_boto3_sso_admin.client import SSOAdminClient
from mypy_boto3_sso_oidc.client import SSOOIDCClient
from mypy_boto3_stepfunctions.client import SFNClient
from mypy_boto3_storagegateway.client import StorageGatewayClient
from mypy_boto3_sts.client import STSClient
from mypy_boto3_support.client import SupportClient
from mypy_boto3_swf.client import SWFClient
from mypy_boto3_synthetics.client import SyntheticsClient
from mypy_boto3_textract.client import TextractClient
from mypy_boto3_timestream_query.client import TimestreamQueryClient
from mypy_boto3_timestream_write.client import TimestreamWriteClient
from mypy_boto3_transcribe.client import TranscribeServiceClient
from mypy_boto3_transfer.client import TransferClient
from mypy_boto3_translate.client import TranslateClient
from mypy_boto3_waf.client import WAFClient
from mypy_boto3_waf_regional.client import WAFRegionalClient
from mypy_boto3_wafv2.client import WAFV2Client
from mypy_boto3_wellarchitected.client import WellArchitectedClient
from mypy_boto3_workdocs.client import WorkDocsClient
from mypy_boto3_worklink.client import WorkLinkClient
from mypy_boto3_workmail.client import WorkMailClient
from mypy_boto3_workmailmessageflow.client import WorkMailMessageFlowClient
from mypy_boto3_workspaces.client import WorkSpacesClient
from mypy_boto3_xray.client import XRayClient

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

class Session:
    def __init__(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        region_name: str = None,
        botocore_session: Session = None,
        profile_name: str = None,
    ):
        self._session: ServiceModel
        self.resource_factory: ResourceFactory
        self._loader: Loader
    def __repr__(self) -> str: ...
    @property
    def profile_name(self) -> str: ...
    @property
    def region_name(self) -> str: ...
    @property
    def events(self) -> List[Any]: ...
    @property
    def available_profiles(self) -> List[Any]: ...
    def _setup_loader(self) -> None: ...
    def get_available_services(self) -> List[str]: ...
    def get_available_resources(self) -> List[str]: ...
    def get_available_partitions(self) -> List[str]: ...
    def get_available_regions(
        self,
        service_name: str,
        partition_name: str = "aws",
        allow_non_regional: bool = False,
    ) -> List[str]: ...
    def get_credentials(self) -> Credentials: ...
    def _register_default_handlers(self) -> None: ...
    @overload
    def client(
        self,
        service_name: Literal["accessanalyzer"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AccessAnalyzerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["acm"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ACMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["acm-pca"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ACMPCAClient: ...
    @overload
    def client(
        self,
        service_name: Literal["alexaforbusiness"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AlexaForBusinessClient: ...
    @overload
    def client(
        self,
        service_name: Literal["amp"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PrometheusServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["amplify"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AmplifyClient: ...
    @overload
    def client(
        self,
        service_name: Literal["amplifybackend"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AmplifyBackendClient: ...
    @overload
    def client(
        self,
        service_name: Literal["apigateway"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> APIGatewayClient: ...
    @overload
    def client(
        self,
        service_name: Literal["apigatewaymanagementapi"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ApiGatewayManagementApiClient: ...
    @overload
    def client(
        self,
        service_name: Literal["apigatewayv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ApiGatewayV2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["appconfig"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppConfigClient: ...
    @overload
    def client(
        self,
        service_name: Literal["appflow"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppflowClient: ...
    @overload
    def client(
        self,
        service_name: Literal["appintegrations"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppIntegrationsServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["application-autoscaling"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ApplicationAutoScalingClient: ...
    @overload
    def client(
        self,
        service_name: Literal["application-insights"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ApplicationInsightsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["appmesh"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppMeshClient: ...
    @overload
    def client(
        self,
        service_name: Literal["appstream"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppStreamClient: ...
    @overload
    def client(
        self,
        service_name: Literal["appsync"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppSyncClient: ...
    @overload
    def client(
        self,
        service_name: Literal["athena"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AthenaClient: ...
    @overload
    def client(
        self,
        service_name: Literal["auditmanager"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AuditManagerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["autoscaling"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AutoScalingClient: ...
    @overload
    def client(
        self,
        service_name: Literal["autoscaling-plans"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AutoScalingPlansClient: ...
    @overload
    def client(
        self,
        service_name: Literal["backup"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> BackupClient: ...
    @overload
    def client(
        self,
        service_name: Literal["batch"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> BatchClient: ...
    @overload
    def client(
        self,
        service_name: Literal["braket"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> BraketClient: ...
    @overload
    def client(
        self,
        service_name: Literal["budgets"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> BudgetsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ce"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CostExplorerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["chime"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ChimeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloud9"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> Cloud9Client: ...
    @overload
    def client(
        self,
        service_name: Literal["clouddirectory"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudDirectoryClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudformation"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudFormationClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudfront"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudFrontClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudhsm"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudHSMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudhsmv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudHSMV2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudsearch"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudSearchClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudsearchdomain"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudSearchDomainClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudtrail"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudTrailClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cloudwatch"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudWatchClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codeartifact"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeArtifactClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codebuild"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeBuildClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codecommit"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeCommitClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codedeploy"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeDeployClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codeguru-reviewer"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeGuruReviewerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codeguruprofiler"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeGuruProfilerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codepipeline"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodePipelineClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codestar"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeStarClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codestar-connections"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeStarconnectionsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["codestar-notifications"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CodeStarNotificationsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cognito-identity"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CognitoIdentityClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cognito-idp"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CognitoIdentityProviderClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cognito-sync"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CognitoSyncClient: ...
    @overload
    def client(
        self,
        service_name: Literal["comprehend"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ComprehendClient: ...
    @overload
    def client(
        self,
        service_name: Literal["comprehendmedical"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ComprehendMedicalClient: ...
    @overload
    def client(
        self,
        service_name: Literal["compute-optimizer"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ComputeOptimizerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["config"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ConfigServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["connect"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ConnectClient: ...
    @overload
    def client(
        self,
        service_name: Literal["connect-contact-lens"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ConnectContactLensClient: ...
    @overload
    def client(
        self,
        service_name: Literal["connectparticipant"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ConnectParticipantClient: ...
    @overload
    def client(
        self,
        service_name: Literal["cur"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CostandUsageReportServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["customer-profiles"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CustomerProfilesClient: ...
    @overload
    def client(
        self,
        service_name: Literal["databrew"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GlueDataBrewClient: ...
    @overload
    def client(
        self,
        service_name: Literal["dataexchange"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DataExchangeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["datapipeline"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DataPipelineClient: ...
    @overload
    def client(
        self,
        service_name: Literal["datasync"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DataSyncClient: ...
    @overload
    def client(
        self,
        service_name: Literal["dax"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DAXClient: ...
    @overload
    def client(
        self,
        service_name: Literal["detective"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DetectiveClient: ...
    @overload
    def client(
        self,
        service_name: Literal["devicefarm"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DeviceFarmClient: ...
    @overload
    def client(
        self,
        service_name: Literal["devops-guru"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DevopsGuruClient: ...
    @overload
    def client(
        self,
        service_name: Literal["directconnect"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DirectConnectClient: ...
    @overload
    def client(
        self,
        service_name: Literal["discovery"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ApplicationDiscoveryServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["dlm"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DLMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["dms"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DatabaseMigrationServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["docdb"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DocDBClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ds"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DirectoryServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["dynamodb"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DynamoDBClient: ...
    @overload
    def client(
        self,
        service_name: Literal["dynamodbstreams"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DynamoDBStreamsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ebs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EBSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ec2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EC2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["ec2-instance-connect"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EC2InstanceConnectClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ecr"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ECRClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ecr-public"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ECRPublicClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ecs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ECSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["efs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EFSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["eks"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EKSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["elastic-inference"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElasticInferenceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["elasticache"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElastiCacheClient: ...
    @overload
    def client(
        self,
        service_name: Literal["elasticbeanstalk"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElasticBeanstalkClient: ...
    @overload
    def client(
        self,
        service_name: Literal["elastictranscoder"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElasticTranscoderClient: ...
    @overload
    def client(
        self,
        service_name: Literal["elb"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElasticLoadBalancingClient: ...
    @overload
    def client(
        self,
        service_name: Literal["elbv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElasticLoadBalancingv2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["emr"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EMRClient: ...
    @overload
    def client(
        self,
        service_name: Literal["emr-containers"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EMRContainersClient: ...
    @overload
    def client(
        self,
        service_name: Literal["es"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ElasticsearchServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["events"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EventBridgeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["firehose"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> FirehoseClient: ...
    @overload
    def client(
        self,
        service_name: Literal["fms"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> FMSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["forecast"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ForecastServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["forecastquery"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ForecastQueryServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["frauddetector"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> FraudDetectorClient: ...
    @overload
    def client(
        self,
        service_name: Literal["fsx"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> FSxClient: ...
    @overload
    def client(
        self,
        service_name: Literal["gamelift"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GameLiftClient: ...
    @overload
    def client(
        self,
        service_name: Literal["glacier"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GlacierClient: ...
    @overload
    def client(
        self,
        service_name: Literal["globalaccelerator"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GlobalAcceleratorClient: ...
    @overload
    def client(
        self,
        service_name: Literal["glue"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GlueClient: ...
    @overload
    def client(
        self,
        service_name: Literal["greengrass"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GreengrassClient: ...
    @overload
    def client(
        self,
        service_name: Literal["greengrassv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GreengrassV2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["groundstation"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GroundStationClient: ...
    @overload
    def client(
        self,
        service_name: Literal["guardduty"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GuardDutyClient: ...
    @overload
    def client(
        self,
        service_name: Literal["health"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> HealthClient: ...
    @overload
    def client(
        self,
        service_name: Literal["healthlake"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> HealthLakeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["honeycode"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> HoneycodeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iam"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IAMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["identitystore"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IdentityStoreClient: ...
    @overload
    def client(
        self,
        service_name: Literal["imagebuilder"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ImagebuilderClient: ...
    @overload
    def client(
        self,
        service_name: Literal["importexport"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ImportExportClient: ...
    @overload
    def client(
        self,
        service_name: Literal["inspector"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> InspectorClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iot"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iot-data"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTDataPlaneClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iot-jobs-data"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTJobsDataPlaneClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iot1click-devices"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoT1ClickDevicesServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iot1click-projects"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoT1ClickProjectsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotanalytics"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTAnalyticsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotdeviceadvisor"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTDeviceAdvisorClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotevents"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTEventsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotevents-data"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTEventsDataClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotfleethub"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTFleetHubClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotsecuretunneling"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTSecureTunnelingClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotsitewise"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTSiteWiseClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotthingsgraph"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTThingsGraphClient: ...
    @overload
    def client(
        self,
        service_name: Literal["iotwireless"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IoTWirelessClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ivs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IVSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kafka"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KafkaClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kendra"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KendraClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesis"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesis-video-archived-media"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisVideoArchivedMediaClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesis-video-media"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisVideoMediaClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesis-video-signaling"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisVideoSignalingChannelsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesisanalytics"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisAnalyticsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesisanalyticsv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisAnalyticsV2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["kinesisvideo"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KinesisVideoClient: ...
    @overload
    def client(
        self,
        service_name: Literal["kms"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> KMSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["lakeformation"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LakeFormationClient: ...
    @overload
    def client(
        self,
        service_name: Literal["lambda"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LambdaClient: ...
    @overload
    def client(
        self,
        service_name: Literal["lex-models"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LexModelBuildingServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["lex-runtime"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LexRuntimeServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["license-manager"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LicenseManagerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["lightsail"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LightsailClient: ...
    @overload
    def client(
        self,
        service_name: Literal["location"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LocationServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["logs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudWatchLogsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["lookoutvision"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> LookoutforVisionClient: ...
    @overload
    def client(
        self,
        service_name: Literal["machinelearning"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MachineLearningClient: ...
    @overload
    def client(
        self,
        service_name: Literal["macie"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MacieClient: ...
    @overload
    def client(
        self,
        service_name: Literal["macie2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> Macie2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["managedblockchain"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ManagedBlockchainClient: ...
    @overload
    def client(
        self,
        service_name: Literal["marketplace-catalog"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MarketplaceCatalogClient: ...
    @overload
    def client(
        self,
        service_name: Literal["marketplace-entitlement"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MarketplaceEntitlementServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["marketplacecommerceanalytics"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MarketplaceCommerceAnalyticsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediaconnect"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaConnectClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediaconvert"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaConvertClient: ...
    @overload
    def client(
        self,
        service_name: Literal["medialive"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaLiveClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediapackage"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaPackageClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediapackage-vod"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaPackageVodClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediastore"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaStoreClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediastore-data"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaStoreDataClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mediatailor"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MediaTailorClient: ...
    @overload
    def client(
        self,
        service_name: Literal["meteringmarketplace"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MarketplaceMeteringClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mgh"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MigrationHubClient: ...
    @overload
    def client(
        self,
        service_name: Literal["migrationhub-config"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MigrationHubConfigClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mobile"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MobileClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mq"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MQClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mturk"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MTurkClient: ...
    @overload
    def client(
        self,
        service_name: Literal["mwaa"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> MWAAClient: ...
    @overload
    def client(
        self,
        service_name: Literal["neptune"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> NeptuneClient: ...
    @overload
    def client(
        self,
        service_name: Literal["network-firewall"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> NetworkFirewallClient: ...
    @overload
    def client(
        self,
        service_name: Literal["networkmanager"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> NetworkManagerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["opsworks"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> OpsWorksClient: ...
    @overload
    def client(
        self,
        service_name: Literal["opsworkscm"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> OpsWorksCMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["organizations"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> OrganizationsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["outposts"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> OutpostsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["personalize"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PersonalizeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["personalize-events"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PersonalizeEventsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["personalize-runtime"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PersonalizeRuntimeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["pi"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PIClient: ...
    @overload
    def client(
        self,
        service_name: Literal["pinpoint"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PinpointClient: ...
    @overload
    def client(
        self,
        service_name: Literal["pinpoint-email"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PinpointEmailClient: ...
    @overload
    def client(
        self,
        service_name: Literal["pinpoint-sms-voice"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PinpointSMSVoiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["polly"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PollyClient: ...
    @overload
    def client(
        self,
        service_name: Literal["pricing"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> PricingClient: ...
    @overload
    def client(
        self,
        service_name: Literal["qldb"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> QLDBClient: ...
    @overload
    def client(
        self,
        service_name: Literal["qldb-session"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> QLDBSessionClient: ...
    @overload
    def client(
        self,
        service_name: Literal["quicksight"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> QuickSightClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ram"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RAMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["rds"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RDSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["rds-data"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RDSDataServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["redshift"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RedshiftClient: ...
    @overload
    def client(
        self,
        service_name: Literal["redshift-data"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RedshiftDataAPIServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["rekognition"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RekognitionClient: ...
    @overload
    def client(
        self,
        service_name: Literal["resource-groups"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ResourceGroupsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["resourcegroupstaggingapi"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ResourceGroupsTaggingAPIClient: ...
    @overload
    def client(
        self,
        service_name: Literal["robomaker"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> RoboMakerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["route53"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> Route53Client: ...
    @overload
    def client(
        self,
        service_name: Literal["route53domains"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> Route53DomainsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["route53resolver"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> Route53ResolverClient: ...
    @overload
    def client(
        self,
        service_name: Literal["s3"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> S3Client: ...
    @overload
    def client(
        self,
        service_name: Literal["s3control"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> S3ControlClient: ...
    @overload
    def client(
        self,
        service_name: Literal["s3outposts"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> S3OutpostsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sagemaker"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SageMakerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sagemaker-a2i-runtime"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AugmentedAIRuntimeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sagemaker-edge"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SagemakerEdgeManagerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sagemaker-featurestore-runtime"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SagemakerFeatureStoreRuntimeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sagemaker-runtime"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SageMakerRuntimeClient: ...
    @overload
    def client(
        self,
        service_name: Literal["savingsplans"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SavingsPlansClient: ...
    @overload
    def client(
        self,
        service_name: Literal["schemas"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SchemasClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sdb"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SimpleDBClient: ...
    @overload
    def client(
        self,
        service_name: Literal["secretsmanager"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SecretsManagerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["securityhub"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SecurityHubClient: ...
    @overload
    def client(
        self,
        service_name: Literal["serverlessrepo"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ServerlessApplicationRepositoryClient: ...
    @overload
    def client(
        self,
        service_name: Literal["service-quotas"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ServiceQuotasClient: ...
    @overload
    def client(
        self,
        service_name: Literal["servicecatalog"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ServiceCatalogClient: ...
    @overload
    def client(
        self,
        service_name: Literal["servicecatalog-appregistry"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> AppRegistryClient: ...
    @overload
    def client(
        self,
        service_name: Literal["servicediscovery"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ServiceDiscoveryClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ses"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SESClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sesv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SESV2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["shield"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> ShieldClient: ...
    @overload
    def client(
        self,
        service_name: Literal["signer"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SignerClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sms"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SMSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sms-voice"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SMSVoiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["snowball"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SnowballClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sns"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SNSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sqs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SQSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["ssm"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SSMClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sso"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SSOClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sso-admin"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SSOAdminClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sso-oidc"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SSOOIDCClient: ...
    @overload
    def client(
        self,
        service_name: Literal["stepfunctions"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SFNClient: ...
    @overload
    def client(
        self,
        service_name: Literal["storagegateway"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> StorageGatewayClient: ...
    @overload
    def client(
        self,
        service_name: Literal["sts"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> STSClient: ...
    @overload
    def client(
        self,
        service_name: Literal["support"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SupportClient: ...
    @overload
    def client(
        self,
        service_name: Literal["swf"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SWFClient: ...
    @overload
    def client(
        self,
        service_name: Literal["synthetics"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SyntheticsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["textract"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> TextractClient: ...
    @overload
    def client(
        self,
        service_name: Literal["timestream-query"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> TimestreamQueryClient: ...
    @overload
    def client(
        self,
        service_name: Literal["timestream-write"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> TimestreamWriteClient: ...
    @overload
    def client(
        self,
        service_name: Literal["transcribe"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> TranscribeServiceClient: ...
    @overload
    def client(
        self,
        service_name: Literal["transfer"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> TransferClient: ...
    @overload
    def client(
        self,
        service_name: Literal["translate"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> TranslateClient: ...
    @overload
    def client(
        self,
        service_name: Literal["waf"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WAFClient: ...
    @overload
    def client(
        self,
        service_name: Literal["waf-regional"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WAFRegionalClient: ...
    @overload
    def client(
        self,
        service_name: Literal["wafv2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WAFV2Client: ...
    @overload
    def client(
        self,
        service_name: Literal["wellarchitected"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WellArchitectedClient: ...
    @overload
    def client(
        self,
        service_name: Literal["workdocs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WorkDocsClient: ...
    @overload
    def client(
        self,
        service_name: Literal["worklink"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WorkLinkClient: ...
    @overload
    def client(
        self,
        service_name: Literal["workmail"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WorkMailClient: ...
    @overload
    def client(
        self,
        service_name: Literal["workmailmessageflow"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WorkMailMessageFlowClient: ...
    @overload
    def client(
        self,
        service_name: Literal["workspaces"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> WorkSpacesClient: ...
    @overload
    def client(
        self,
        service_name: Literal["xray"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> XRayClient: ...
    @overload
    def resource(
        self,
        service_name: Literal["cloudformation"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudFormationServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["cloudwatch"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> CloudWatchServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["dynamodb"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> DynamoDBServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["ec2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EC2ServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["glacier"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> GlacierServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["iam"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> IAMServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["opsworks"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> OpsWorksServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["s3"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> S3ServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["sns"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SNSServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["sqs"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> SQSServiceResource: ...
