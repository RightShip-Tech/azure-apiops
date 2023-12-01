using common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace publisher;

internal class Publisher : BackgroundService
{
    internal record Parameters
    {
        public required IHostApplicationLifetime ApplicationLifetime { get; init; }
        public CommitId? FromCommitId { get; init; }
        public CommitId? ToCommitId { get; init; }
        public FileInfo? ConfigurationFile { get; init; }
        public required JsonObject ConfigurationJson { get; init; }
        public required DeleteRestResource DeleteRestResource { get; init; }
        public required ILogger Logger { get; init; }
        public required ListRestResources ListRestResources { get; init; }
        public required PutRestResource PutRestResource { get; init; }
        public required ServiceDirectory ServiceDirectory { get; init; }
        public required ServiceUri ServiceUri { get; init; }
    }

    private readonly Parameters publisherParameters;

    public Publisher(Parameters publisherParameters)
    {
        this.publisherParameters = publisherParameters;
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var logger = publisherParameters.Logger;

        try
        {
            logger.LogInformation("Beginning execution...");

            await Run(cancellationToken);

            logger.LogInformation("Execution complete.");
        }
        catch (OperationCanceledException)
        {
            // Don't throw if operation was canceled
        }
        catch (Exception exception)
        {
            logger.LogCritical(exception, "");
            Environment.ExitCode = -1;
            throw;
        }
        finally
        {
            publisherParameters.ApplicationLifetime.StopApplication();
        }
    }

    private async ValueTask Run(CancellationToken cancellationToken)
    {
        await (publisherParameters.FromCommitId is null || publisherParameters.ToCommitId is null
                ? RunWithoutCommitId(cancellationToken)
                : RunWithCommitId(publisherParameters.FromCommitId, publisherParameters.ToCommitId, cancellationToken));
    }

    private async ValueTask RunWithoutCommitId(CancellationToken cancellationToken)
    {
        var logger = publisherParameters.Logger;

        logger.LogInformation("Commit ID was not specified, will put all artifact files...");

        var files = publisherParameters.ServiceDirectory
                                       .EnumerateFilesRecursively()
                                       .ToImmutableList();

        await Service.ProcessArtifactsToPut(files,
                                            publisherParameters.ConfigurationJson,
                                            publisherParameters.ServiceDirectory,
                                            publisherParameters.ServiceUri,
                                            publisherParameters.ListRestResources,
                                            publisherParameters.PutRestResource,
                                            publisherParameters.DeleteRestResource,
                                            logger,
                                            cancellationToken);
    }

    private async ValueTask RunWithCommitId(CommitId fromCommitId, CommitId toCommitId, CancellationToken cancellationToken)
    {
        var logger = publisherParameters.Logger;

        logger.LogInformation("Getting files from commit IDs {fromCommitId}-{toCommitId}...", fromCommitId, toCommitId);
        var fileDictionary = await GetCommitIdFiles(fromCommitId, toCommitId);

        if (fileDictionary.TryGetValue(Action.Put, out var putFiles) && putFiles.Any())
        {
            logger.LogInformation("Processing modified files in commit ID...");
            await ProcessCommitIdFilesToPut(putFiles, cancellationToken);
        }

        if (fileDictionary.TryGetValue(Action.Delete, out var deletedFiles) && deletedFiles.Any())
        {
            logger.LogInformation("Processing files deleted in commit ID...");
            await ProcessDeletedCommitIdFiles(deletedFiles, cancellationToken);
        }
    }

    private async ValueTask<ImmutableDictionary<Action, ImmutableList<FileInfo>>> GetCommitIdFiles(CommitId fromCommitId, CommitId toCommitId)
    {
        var serviceDirectoryInfo = publisherParameters.ServiceDirectory.GetDirectoryInfo();
        var commitDictionary = await Git.GetFilesFromCommit(fromCommitId, toCommitId, serviceDirectoryInfo);

        var fileActions = from kvp in commitDictionary
                          from fileAction in from file in kvp.Value
                                             let commitStatus = kvp.Key
                                             let action = MatchCommitStatusToAction(commitStatus)
                                             select (File: file, Action: action)
                          group fileAction.File by fileAction.Action;

        return fileActions.ToImmutableDictionary(group => group.Key,
                                                 group => group.DistinctBy(file => file.FullName)
                                                               .ToImmutableList());
    }

    private static Action MatchCommitStatusToAction(CommitStatus commitStatus) =>
        commitStatus == CommitStatus.Delete
        ? Action.Delete
        : Action.Put;

    private async ValueTask ProcessDeletedCommitIdFiles(IReadOnlyCollection<FileInfo> deletedCommitIdFiles, CancellationToken cancellationToken)
    {
        await Service.ProcessDeletedArtifacts(deletedCommitIdFiles,
                                              publisherParameters.ConfigurationJson,
                                              publisherParameters.ServiceDirectory,
                                              publisherParameters.ServiceUri,
                                              publisherParameters.ListRestResources,
                                              publisherParameters.PutRestResource,
                                              publisherParameters.DeleteRestResource,
                                              publisherParameters.Logger,
                                              cancellationToken);
    }

    private async ValueTask ProcessCommitIdFilesToPut(IReadOnlyCollection<FileInfo> commitIdFilesToPut, CancellationToken cancellationToken)
    {
        await Service.ProcessArtifactsToPut(commitIdFilesToPut,
                                            publisherParameters.ConfigurationJson,
                                            publisherParameters.ServiceDirectory,
                                            publisherParameters.ServiceUri,
                                            publisherParameters.ListRestResources,
                                            publisherParameters.PutRestResource,
                                            publisherParameters.DeleteRestResource,
                                            publisherParameters.Logger,
                                            cancellationToken);
    }

    private enum Action
    {
        Put,
        Delete
    }
}