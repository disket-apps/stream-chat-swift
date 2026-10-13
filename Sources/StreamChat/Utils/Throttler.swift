//
// Copyright © 2025 Stream.io Inc. All rights reserved.
//

import Foundation

/// A throttler implementation. The action provided will only be executed if the last action executed has passed an amount of time.
///
/// The API is based on the implementation from Apple:
/// https://developer.apple.com/documentation/combine/anypublisher/throttle(for:scheduler:latest:)
public class Throttler {
    // `execute` is called from the main thread while the work item runs on `queue`: every access to the
    // mutable state goes through `lock`. Unsynchronised, the two threads raced on `workItem` and freed a
    // DispatchWorkItem still in use (SIGABRT « deallocated with non-zero retain count », EXC_BAD_ACCESS).
    private let lock = NSLock()
    private var workItem: DispatchWorkItem?
    private let queue: DispatchQueue
    private var previousRun: Date = Date.distantPast
    private let broadcastLatestEvent: Bool

    /// The current interval that an action can be executed.
    public var interval: TimeInterval

    /// - Parameters:
    ///   - interval: The interval that an action can be executed.
    ///   - broadcastLatestEvent: A Boolean value that indicates whether we should be using the first or last event of the ones that are being throttled.
    ///   - queue: The queue where the work will be executed.
    ///   This last action will have a delay of the provided interval until it is executed.
    public init(
        interval: TimeInterval,
        broadcastLatestEvent: Bool = true,
        queue: DispatchQueue = .init(label: "com.stream.throttler", qos: .utility)
    ) {
        self.interval = interval
        self.broadcastLatestEvent = broadcastLatestEvent
        self.queue = queue
    }

    /// Throttle an action. It will cancel the previous action if exists, and it will execute the action immediately
    /// if the last action executed was past the interval provided. If not, it will only be executed after a delay.
    /// - Parameter action: The closure to be performed.
    public func execute(_ action: @escaping () -> Void) {
        let token = Token()
        let workItem = DispatchWorkItem { [weak self] in
            guard let self else { return }
            self.lock.lock()
            let isCurrent = self.workItem.map { !$0.isCancelled && self.currentToken === token } ?? false
            self.lock.unlock()
            guard isCurrent else { return }
            action()
            self.lock.lock()
            self.previousRun = Date()
            if self.currentToken === token {
                self.workItem = nil
                self.currentToken = nil
            }
            self.lock.unlock()
        }

        lock.lock()
        self.workItem?.cancel()
        self.workItem = workItem
        self.currentToken = token
        let timeSinceLastRun = Date().timeIntervalSince(previousRun)
        let currentInterval = interval
        lock.unlock()

        let delay = timeSinceLastRun > currentInterval ? 0 : currentInterval
        // If the delay is 0, we always execute the action immediately.
        // If the delay is bigger than 0, we only execute it if `latest` was enabled.
        if delay == 0 || delay > 0 && broadcastLatestEvent {
            queue.asyncAfter(deadline: .now() + delay, execute: workItem)
        }
    }

    /// Cancel any active action.
    public func cancel() {
        lock.lock()
        workItem?.cancel()
        workItem = nil
        currentToken = nil
        lock.unlock()
    }

    /// Identifies the latest scheduled action without the work item capturing itself.
    private final class Token {}
    private var currentToken: Token?
}
