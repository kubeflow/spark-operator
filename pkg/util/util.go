/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"encoding/base64"
	"hash"
	"hash/fnv"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewHash32 returns a 32-bit hash computed from the given byte slice.
func NewHash32() hash.Hash32 {
	return fnv.New32()
}

func encodeToString(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}

func decodeString(value string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(value)
}

// MarshalVolume encodes the given Volume into a string.
func MarshalVolume(volume *apiv1.Volume) (string, error) {
	volumeData, err := volume.Marshal()
	if err != nil {
		return "", err
	}
	return encodeToString(volumeData), nil
}

// UnmarshalVolume decodes a Volume from the given string.
func UnmarshalVolume(volumeStr string) (*apiv1.Volume, error) {
	volume := &apiv1.Volume{}
	decoded, err := decodeString(volumeStr)
	if err != nil {
		return nil, err
	}
	if err = volume.Unmarshal(decoded); err != nil {
		return nil, err
	}
	return volume, nil
}

// MarshalVolumeMount encodes the given VolumeMount into a string.
func MarshalVolumeMount(mount *apiv1.VolumeMount) (string, error) {
	mountData, err := mount.Marshal()
	if err != nil {
		return "", err
	}
	return encodeToString(mountData), nil
}

// UnmarshalVolumeMount decodes a VolumeMount from the given string.
func UnmarshalVolumeMount(mountStr string) (*apiv1.VolumeMount, error) {
	mount := &apiv1.VolumeMount{}
	decoded, err := decodeString(mountStr)
	if err != nil {
		return nil, err
	}
	if err = mount.Unmarshal(decoded); err != nil {
		return nil, err
	}
	return mount, nil
}

// MarshalOwnerReference encodes the given OwnerReference into a string.
func MarshalOwnerReference(reference *metav1.OwnerReference) (string, error) {
	referenceData, err := reference.Marshal()
	if err != nil {
		return "", err
	}
	return encodeToString(referenceData), nil
}

// UnmarshalOwnerReference decodes a OwnerReference from the given string.
func UnmarshalOwnerReference(ownerReferenceStr string) (*metav1.OwnerReference, error) {
	ownerReference := &metav1.OwnerReference{}
	decoded, err := decodeString(ownerReferenceStr)
	if err != nil {
		return nil, err
	}
	if err = ownerReference.Unmarshal(decoded); err != nil {
		return nil, err
	}
	return ownerReference, nil
}
