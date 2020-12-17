package com.box.dataplatform.iceberg.core.testkit.kms;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.model.*;
import com.box.dataplatform.crypto.BCEncryptor;
import com.box.dataplatform.crypto.Encryptor;
import com.box.dataplatform.iceberg.encryption.crypto.AesGcmFormat;
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit;
import java.io.Serializable;
import java.lang.UnsupportedOperationException;
import java.nio.ByteBuffer;

public class MockKmsClient implements Serializable, AWSKMS {
  Encryptor encryptor;
  public MockKmsClient() {
    encryptor = BCEncryptor.INSTANCE;
  }

  @Override
  public void setEndpoint(String endpoint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRegion(Region region) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CancelKeyDeletionResult cancelKeyDeletion(
      CancelKeyDeletionRequest cancelKeyDeletionRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConnectCustomKeyStoreResult connectCustomKeyStore(
      ConnectCustomKeyStoreRequest connectCustomKeyStoreRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateAliasResult createAlias(CreateAliasRequest createAliasRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateCustomKeyStoreResult createCustomKeyStore(
      CreateCustomKeyStoreRequest createCustomKeyStoreRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateGrantResult createGrant(CreateGrantRequest createGrantRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateKeyResult createKey(CreateKeyRequest createKeyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateKeyResult createKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DecryptResult decrypt(DecryptRequest decryptRequest) {
    byte[] result = new byte[0];
    try {
      result =
          encryptor
              .ctrDecryptionBuilder(KmsTestkit.STATIC_DEK)
              .build()
              .doFinal(decryptRequest.getCiphertextBlob().array());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new DecryptResult()
        .withKeyId(DPIcebergTestkit.KMS_KEY_ID)
        .withPlaintext(ByteBuffer.wrap(result));
  }

  @Override
  public DeleteAliasResult deleteAlias(DeleteAliasRequest deleteAliasRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteCustomKeyStoreResult deleteCustomKeyStore(
      DeleteCustomKeyStoreRequest deleteCustomKeyStoreRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeleteImportedKeyMaterialResult deleteImportedKeyMaterial(
      DeleteImportedKeyMaterialRequest deleteImportedKeyMaterialRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeCustomKeyStoresResult describeCustomKeyStores(
      DescribeCustomKeyStoresRequest describeCustomKeyStoresRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeKeyResult describeKey(DescribeKeyRequest describeKeyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DisableKeyResult disableKey(DisableKeyRequest disableKeyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DisableKeyRotationResult disableKeyRotation(
      DisableKeyRotationRequest disableKeyRotationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DisconnectCustomKeyStoreResult disconnectCustomKeyStore(
      DisconnectCustomKeyStoreRequest disconnectCustomKeyStoreRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnableKeyResult enableKey(EnableKeyRequest enableKeyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnableKeyRotationResult enableKeyRotation(
      EnableKeyRotationRequest enableKeyRotationRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EncryptResult encrypt(EncryptRequest encryptRequest) {
    byte[] blob = new byte[0];
    try {
      blob =
          encryptor
              .ctrEncryptionBuilder(KmsTestkit.STATIC_DEK)
              .build()
              .doFinal(encryptRequest.getPlaintext().array());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new EncryptResult()
        .withKeyId(DPIcebergTestkit.KMS_KEY_ID)
        .withCiphertextBlob(ByteBuffer.wrap(blob));
  }

  @Override
  public GenerateDataKeyResult generateDataKey(GenerateDataKeyRequest generateDataKeyRequest) {
    byte[] plaintext = BCEncryptor.INSTANCE.generateRandomBytes(AesGcmFormat.DEK_LENGTH);
    byte[] encrypted = null;
    try {
      encrypted =
          encryptor
              .ctrEncryptionBuilder(KmsTestkit.STATIC_DEK)
              .build()
              .doFinal(plaintext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new GenerateDataKeyResult()
        .withKeyId(generateDataKeyRequest.getKeyId())
        .withPlaintext(ByteBuffer.wrap(plaintext))
        .withCiphertextBlob(ByteBuffer.wrap(encrypted));
  }

  @Override
  public GenerateDataKeyWithoutPlaintextResult generateDataKeyWithoutPlaintext(
      GenerateDataKeyWithoutPlaintextRequest generateDataKeyWithoutPlaintextRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GenerateRandomResult generateRandom(GenerateRandomRequest generateRandomRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GenerateRandomResult generateRandom() {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetKeyPolicyResult getKeyPolicy(GetKeyPolicyRequest getKeyPolicyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetKeyRotationStatusResult getKeyRotationStatus(
      GetKeyRotationStatusRequest getKeyRotationStatusRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetParametersForImportResult getParametersForImport(
      GetParametersForImportRequest getParametersForImportRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ImportKeyMaterialResult importKeyMaterial(
      ImportKeyMaterialRequest importKeyMaterialRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListAliasesResult listAliases(ListAliasesRequest listAliasesRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListAliasesResult listAliases() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListGrantsResult listGrants(ListGrantsRequest listGrantsRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListKeyPoliciesResult listKeyPolicies(ListKeyPoliciesRequest listKeyPoliciesRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListKeysResult listKeys(ListKeysRequest listKeysRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListKeysResult listKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListResourceTagsResult listResourceTags(ListResourceTagsRequest listResourceTagsRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListRetirableGrantsResult listRetirableGrants(
      ListRetirableGrantsRequest listRetirableGrantsRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PutKeyPolicyResult putKeyPolicy(PutKeyPolicyRequest putKeyPolicyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReEncryptResult reEncrypt(ReEncryptRequest reEncryptRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RetireGrantResult retireGrant(RetireGrantRequest retireGrantRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RetireGrantResult retireGrant() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RevokeGrantResult revokeGrant(RevokeGrantRequest revokeGrantRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduleKeyDeletionResult scheduleKeyDeletion(
      ScheduleKeyDeletionRequest scheduleKeyDeletionRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TagResourceResult tagResource(TagResourceRequest tagResourceRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UntagResourceResult untagResource(UntagResourceRequest untagResourceRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UpdateAliasResult updateAlias(UpdateAliasRequest updateAliasRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UpdateCustomKeyStoreResult updateCustomKeyStore(
      UpdateCustomKeyStoreRequest updateCustomKeyStoreRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UpdateKeyDescriptionResult updateKeyDescription(
      UpdateKeyDescriptionRequest updateKeyDescriptionRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    throw new UnsupportedOperationException();
  }
}
