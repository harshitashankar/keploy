//go:build linux

package generic

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"

	"go.keploy.io/server/v2/pkg"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.uber.org/zap"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations/util"
	"go.keploy.io/server/v2/pkg/models"
)

// maskUUIDs replaces UUID patterns in the byte array with a placeholder
// UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 characters)
// This is a temporary fix to ignore UUID differences in matching
func maskUUIDs(data []byte) []byte {
	// UUID regex pattern: 8 hex digits, hyphen, 4 hex digits, hyphen, 4 hex digits, hyphen, 4 hex digits, hyphen, 12 hex digits
	uuidPattern := regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	
	// Convert to string, mask UUIDs, convert back to bytes
	str := string(data)
	masked := uuidPattern.ReplaceAllString(str, "00000000-0000-0000-0000-000000000000")
	return []byte(masked)
}

// fuzzyMatch performs a fuzzy matching algorithm to find the best matching mock for the given request.
// It takes a context, a request buffer, and a mock database as input parameters.
// The function iterates over the mocks in the database and applies the fuzzy matching algorithm to find the best match.
// If a match is found, it returns the corresponding response mock and a boolean value indicating success.
// If no match is found, it returns false and a nil response.
// If an error occurs during the matching process, it returns an error.
func fuzzyMatch(ctx context.Context, logger *zap.Logger, reqBuff [][]byte, mockDb integrations.MockMemDb) (bool, []models.Payload, error) {
	for {
		select {
		case <-ctx.Done():
			return false, nil, ctx.Err()
		default:
			mocks, err := mockDb.GetUnFilteredMocks()
			if err != nil {
				return false, nil, fmt.Errorf("error while getting unfiltered mocks %v", err)
			}

			var filteredMocks []*models.Mock
			var unfilteredMocks []*models.Mock

			for _, mock := range mocks {
				if mock.Kind != "Generic" {
					continue
				}
				if mock.TestModeInfo.IsFiltered {
					filteredMocks = append(filteredMocks, mock)
				} else {
					unfilteredMocks = append(unfilteredMocks, mock)
				}
			}

			logger.Debug("List of mocks in the database", zap.Int("Filtered Mocks", len(filteredMocks)), zap.Int("Unfiltered Mocks", len(unfilteredMocks)))
			for i, mock := range filteredMocks {
				logger.Debug("Filtered Mocks", zap.String(fmt.Sprintf("Mock[%d]", i), mock.Name), zap.Int64("sortOrder", mock.TestModeInfo.SortOrder))
			}
			for i, mock := range unfilteredMocks {
				logger.Debug("Unfiltered Mocks", zap.String(fmt.Sprintf("Mock[%d]", i), mock.Name), zap.Int64("sortOrder", mock.TestModeInfo.SortOrder))
			}

			index := findExactMatch(filteredMocks, reqBuff)

			if index == -1 {
				index = findBinaryMatch(filteredMocks, reqBuff, 0.9)
			}

			if index != -1 {
				responseMock := make([]models.Payload, len(filteredMocks[index].Spec.GenericResponses))
				copy(responseMock, filteredMocks[index].Spec.GenericResponses)
				originalFilteredMock := *filteredMocks[index]
				filteredMocks[index].TestModeInfo.IsFiltered = false
				filteredMocks[index].TestModeInfo.SortOrder = pkg.GetNextSortNum()
				isUpdated := mockDb.UpdateUnFilteredMock(&originalFilteredMock, filteredMocks[index])
				if !isUpdated {
					continue
				}
				logger.Debug("Filtered mock found for generic request", zap.String("Mock", filteredMocks[index].Name), zap.Int64("sortOrder", filteredMocks[index].TestModeInfo.SortOrder))
				return true, responseMock, nil
			}

			index = findExactMatch(unfilteredMocks, reqBuff)

			if index == -1 {
				index = findBinaryMatch(unfilteredMocks, reqBuff, 0.4)
			}
			if index != -1 {
				responseMock := make([]models.Payload, len(unfilteredMocks[index].Spec.GenericResponses))
				copy(responseMock, unfilteredMocks[index].Spec.GenericResponses)
				originalFilteredMock := *unfilteredMocks[index]
				unfilteredMocks[index].TestModeInfo.IsFiltered = false
				unfilteredMocks[index].TestModeInfo.SortOrder = pkg.GetNextSortNum()
				isUpdated := mockDb.UpdateUnFilteredMock(&originalFilteredMock, unfilteredMocks[index])
				if !isUpdated {
					continue
				}
				logger.Debug("Unfiltered mock found for generic request", zap.String("Mock", unfilteredMocks[index].Name), zap.Int64("sortOrder", unfilteredMocks[index].TestModeInfo.SortOrder))
				return true, responseMock, nil
			}
			return false, nil, nil
		}
	}
}

// TODO: need to generalize this function for different types of integrations.
func findBinaryMatch(tcsMocks []*models.Mock, reqBuffs [][]byte, mxSim float64) int {
	// TODO: need find a proper similarity index to set a benchmark for matching or need to find another way to do approximate matching
	mxIdx := -1
	for idx, mock := range tcsMocks {
		if len(mock.Spec.GenericRequests) == len(reqBuffs) {
			for requestIndex, reqBuff := range reqBuffs {
				_ = base64.StdEncoding.EncodeToString(reqBuff)
				encoded, _ := util.DecodeBase64(mock.Spec.GenericRequests[requestIndex].Message[0].Data)

				similarity := fuzzyCheck(encoded, reqBuff)

				if mxSim < similarity {
					mxSim = similarity
					mxIdx = idx
				}
			}
		}
	}
	return mxIdx
}

func fuzzyCheck(encoded, reqBuf []byte) float64 {
	// Mask UUIDs before comparison to ignore UUID differences
	encodedMasked := maskUUIDs(encoded)
	reqBufMasked := maskUUIDs(reqBuf)
	
	k := util.AdaptiveK(len(reqBufMasked), 3, 8, 5)
	shingles1 := util.CreateShingles(encodedMasked, k)
	shingles2 := util.CreateShingles(reqBufMasked, k)
	similarity := util.JaccardSimilarity(shingles1, shingles2)
	return similarity
}

func findExactMatch(tcsMocks []*models.Mock, reqBuffs [][]byte) int {
	for idx, mock := range tcsMocks {
		if len(mock.Spec.GenericRequests) == len(reqBuffs) {
			matched := true // Flag to track if all requests match

			for requestIndex, reqBuff := range reqBuffs {
				// Get mock data
				mockData := mock.Spec.GenericRequests[requestIndex].Message[0].Data
				mockType := mock.Spec.GenericRequests[requestIndex].Message[0].Type
				
				// Decode mock data if it's binary
				var mockBytes []byte
				if mockType == "binary" {
					mockBytes, _ = util.DecodeBase64(mockData)
				} else {
					mockBytes = []byte(mockData)
				}
				
				// Prepare request bytes for comparison
				var reqBytes []byte
				bufStr := string(reqBuff)
				if !util.IsASCII(string(reqBuff)) {
					// If not ASCII, compare as base64 encoded
					bufStr = util.EncodeBase64(reqBuff)
					reqBytes = []byte(bufStr)
				} else {
					reqBytes = reqBuff
				}
				
				// Mask UUIDs in both before comparison
				mockMasked := maskUUIDs(mockBytes)
				reqMasked := maskUUIDs(reqBytes)
				
				// Compare masked data
				if string(mockMasked) != string(reqMasked) {
					matched = false
					break // Exit the loop if any request doesn't match
				}
			}

			if matched {
				return idx
			}
		}
	}
	return -1
}
