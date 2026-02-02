// Bootstrap commands for the synchronizer service
// Add synchronizer initialization commands here
def main() = {

  sequencer1.health.wait_for_ready_for_initialization()
  mediator1.health.wait_for_ready_for_initialization()
  nodes.local.start()
  bootstrap.synchronizer(
    synchronizerName = "da",
    sequencers = Seq(sequencer1),
    mediators = Seq(mediator1),
    synchronizerOwners = Seq(sequencer1),
    synchronizerThreshold = PositiveInt.one,
    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer),
  )
  logger.info("=== Bootstrapping synchronizer complete ===")
}
