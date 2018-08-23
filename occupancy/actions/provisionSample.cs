using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using YamlDotNet.Serialization;

namespace Microsoft.Azure.DigitalTwins.Samples
{
    public static partial class Actions
    {
        static private Deserializer _yamlDeserializer = new Deserializer();

        public static async Task<IEnumerable<Guid>> ProvisionSample(HttpClient httpClient, Logger logger)
        {
            IEnumerable<SpaceDescription> spaceCreateDescriptions;
            using (var r = new StreamReader("actions/provisionSample.yaml"))
            {
                spaceCreateDescriptions = await GetProvisionSampleTopology(r);
            }
            var createdSpaceIds = await CreateSpaces(httpClient, logger, spaceCreateDescriptions, Guid.Empty);
            var createdSpaceIdsAsString = createdSpaceIds
                .Select(x => x.ToString())
                .Aggregate((acc, cur) => acc + ", " + cur);
            logger.WriteLine($"Created spaces: {createdSpaceIdsAsString}");
            return createdSpaceIds;
        }

        public static async Task<IEnumerable<SpaceDescription>> GetProvisionSampleTopology(TextReader textReader)
            => _yamlDeserializer.Deserialize<IEnumerable<SpaceDescription>>(await textReader.ReadToEndAsync());

        public static async Task<IEnumerable<Guid>> CreateSpaces(
            HttpClient httpClient,
            Logger logger,
            IEnumerable<SpaceDescription> descriptions,
            Guid parentId)
        {
            var spaceIds = new List<Guid>();
            foreach (var description in descriptions)
            {
                var spaceCreate = new Models.SpaceCreate()
                {
                    Name = description.name,
                    ParentSpaceId = parentId != Guid.Empty ? parentId.ToString() : "",
                };

                var existingSpace = await GetUnqiueSpace(httpClient, logger, description.name, parentId);
                var spaceId = existingSpace?.Id != null ? Guid.Parse(existingSpace.Id) : await CreateSpace(httpClient, logger, spaceCreate);
                logger.WriteLine("");

                if (spaceId != Guid.Empty)
                {
                    spaceIds.Add(spaceId);

                    if (description.spaces != null)
                        await CreateSpaces(httpClient, logger, description.spaces, spaceId);

                    // TODO: other children types
                }
            }

            return spaceIds;
        }

        /// Returns a space with same name and parentId if there is exactly one
        /// Otherwise returns null
        private static async Task<Models.Space> GetUnqiueSpace(
            HttpClient httpClient,
            Logger logger,
            string name,
            Guid parentId)
        {
            var filterName = $"Name eq '{name}'";
            var filterParentSpaceId = parentId != Guid.Empty
                ? $"ParentSpaceId eq guid'{parentId}'"
                : $"ParentSpaceId eq null";
            var odataFilter = $"$filter={filterName} and {filterParentSpaceId}";

            var response = await httpClient.GetAsync($"spaces?{odataFilter}");
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                var spaces = JsonConvert.DeserializeObject<IReadOnlyCollection<Models.Space>>(content);
                var matchingSpace = spaces.Count == 1 ? spaces.First() : null;
                if (matchingSpace != null)
                {
                    logger.WriteLine($"Retrieved Unique Space using 'name' and 'parentSpaceId': {JsonConvert.SerializeObject(matchingSpace, Formatting.Indented)}");
                    return matchingSpace;
                }
            }
            return null;
        }

        private static async Task<Guid> CreateSpace(HttpClient httpClient, Logger logger, Models.SpaceCreate spaceCreate)
        {
            logger.WriteLine($"Creating Space: {JsonConvert.SerializeObject(spaceCreate, Formatting.Indented)}");
            var content = JsonConvert.SerializeObject(spaceCreate);
            var response = await httpClient.PostAsync("spaces", new StringContent(content, Encoding.UTF8, "application/json"));
            return await GetIdFromResponse(response, logger);
        }

        private static async Task<Guid> GetIdFromResponse(HttpResponseMessage response, Logger logger)
        {
            if (!response.IsSuccessStatusCode)
                return Guid.Empty;
            var content = await response.Content.ReadAsStringAsync();

            // strip out the double quotes that come in the response
            var contentSanitized = content.Substring(1, content.Length - 2);

            if (!Guid.TryParse(contentSanitized, out var createdId))
            {
                logger.WriteLine($"ERROR: Returned value from POST did not parse into a guid: {content}");
                return Guid.Empty;
            }

            return createdId;
        }
    }

    public class DeviceDescription
    {
        public string name { get; set; }
        public string hardwareId { get; set; }
    }

    public class ResourceDescription
    {
        public string region { get; set; }
        public string type { get; set; }
    }

    public class SpaceDescription
    {
        public string name { get; set; }
        public string type { get; set; }
        public string subType { get; set; }

        [JsonIgnore]
        public IEnumerable<DeviceDescription> devices { get; set; }

        [JsonIgnore]
        public IEnumerable<ResourceDescription> resources { get; set; }

        [JsonIgnore]
        public IEnumerable<SpaceDescription> spaces { get; set; }
    }
}