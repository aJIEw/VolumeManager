package moe.chensi.volume.data

import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.PreferenceDataStoreFactory
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.emptyPreferences
import androidx.datastore.preferences.core.preferencesOf
import androidx.datastore.preferences.core.stringPreferencesKey
import java.io.File
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.float
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

@OptIn(FlowPreview::class)
class AppPreferencesStoreTest {
    @get:Rule
    val temporaryFolder = TemporaryFolder()

    private val dataStoreScopes = mutableListOf<CoroutineScope>()

    @After
    fun tearDown() {
        dataStoreScopes.forEach(CoroutineScope::cancel)
    }

    @Test
    fun persistedStateRemainsCompatible() = runBlocking {
        val dataStore = InMemoryDataStore(preferencesOf(AppsKey to ExistingStateJson))

        val store = AppPreferencesStore(dataStore)
        val loaded = CompletableDeferred<Unit>()
        store.loadOnce { loaded.complete(Unit) }
        withTimeout(5_000) { loaded.await() }

        val appPreferences = store.getOrCreate("example.package")
        assertEquals(AppPreferences(true, 0.42f, true, true), appPreferences)
        assertEquals(mapOf("media" to false), store.systemSliderVisibility)

        appPreferences.isPlayer = false
        appPreferences.volume = 0.75f
        appPreferences.hidden = false
        appPreferences.disableVolumeButtons = false
        store.systemSliderVisibility = mapOf("media" to true, "alarm" to false)
        store.save()

        withTimeout(10_000) {
            dataStore.data.first { preferences ->
                readFirstVolume(preferences) == 0.75f
            }
        }

        val restoredStore = AppPreferencesStore(dataStore)
        val restored = CompletableDeferred<Unit>()
        restoredStore.loadOnce { restored.complete(Unit) }
        withTimeout(5_000) { restored.await() }

        assertEquals(
            AppPreferences(false, 0.75f, false, false),
            restoredStore.getOrCreate("example.package")
        )
        assertEquals(
            mapOf("media" to true, "alarm" to false),
            restoredStore.systemSliderVisibility
        )
    }

    @Test
    fun loadOnceInitializesOnlyOnce() = runBlocking {
        val dataStore = InMemoryDataStore()
        val store = AppPreferencesStore(dataStore)
        val callbackCount = AtomicInteger()
        val loaded = CompletableDeferred<Unit>()

        store.loadOnce {
            callbackCount.incrementAndGet()
            loaded.complete(Unit)
        }
        store.loadOnce {
            callbackCount.incrementAndGet()
        }
        withTimeout(5_000) { loaded.await() }

        dataStore.edit { preferences ->
            preferences[AppsKey] = EmptyStateJson
        }
        delay(250)

        assertEquals(1, callbackCount.get())
    }

    @Test
    fun loadCanRetryAfterOneFailure() = runBlocking {
        val dataStore = FailFirstLoadDataStore()
        val store = AppPreferencesStore(dataStore)
        val callbackCount = AtomicInteger()
        val loaded = CompletableDeferred<Unit>()

        store.loadOnce { callbackCount.incrementAndGet() }
        withTimeout(5_000) { dataStore.firstFailureObserved.await() }

        withTimeout(5_000) {
            while (!loaded.isCompleted) {
                store.loadOnce {
                    callbackCount.incrementAndGet()
                    loaded.complete(Unit)
                }
                delay(10)
            }
        }

        assertEquals(1, callbackCount.get())
    }

    @Test
    fun saveContinuesAfterOneWriteFails() = runBlocking {
        val dataStore = FailFirstUpdateDataStore()
        val store = AppPreferencesStore(dataStore)
        val loaded = CompletableDeferred<Unit>()
        store.loadOnce { loaded.complete(Unit) }
        withTimeout(5_000) { loaded.await() }

        val preferences = store.getOrCreate("example.package")
        preferences.volume = 0.25f
        store.save()
        withTimeout(5_000) { dataStore.firstFailureObserved.await() }

        preferences.volume = 1f
        store.save()
        withTimeout(5_000) { dataStore.successfulUpdateObserved.await() }

        val persistedVolume = readFirstVolume(dataStore.currentPreferences) ?: Float.NaN
        assertEquals(1f, persistedVolume, 0f)
    }

    @Test
    fun newerSaveWinsWhenOlderWriteEmitsFirst() = runBlocking {
        val dataStore = ControlledDataStore()
        val store = AppPreferencesStore(dataStore)
        val loaded = CompletableDeferred<Unit>()
        store.loadOnce { loaded.complete(Unit) }
        withTimeout(5_000) { loaded.await() }

        val preferences = store.getOrCreate("example.package")
        preferences.volume = 0.25f
        store.save()
        withTimeout(5_000) { dataStore.firstUpdateTransformed.await() }

        preferences.volume = 1f
        store.save()
        dataStore.allowFirstEmission.complete(Unit)
        withTimeout(5_000) { dataStore.firstEmissionPublished.await() }

        val staleReplayObserved = withTimeoutOrNull(1_000) {
            while (store.getOrCreate("example.package").volume != 0.25f) {
                yield()
            }
            true
        } ?: false
        val runtimeVolume = store.getOrCreate("example.package").volume

        dataStore.allowFirstUpdateToFinish.complete(Unit)
        withTimeout(5_000) { dataStore.secondUpdateFinished.await() }

        assertFalse(staleReplayObserved)
        assertEquals(1f, runtimeVolume, 0f)
        val persistedVolume = readFirstVolume(dataStore.currentPreferences) ?: Float.NaN
        assertEquals(1f, persistedVolume, 0f)
    }

    @Test
    fun rapidSavesPersistLatestVolume() = runBlocking {
        val dataStore = createDataStore("rapid-saves.preferences_pb")
        val store = AppPreferencesStore(dataStore)
        val loaded = CompletableDeferred<Unit>()
        store.loadOnce { loaded.complete(Unit) }
        withTimeout(5_000) { loaded.await() }

        val preferences = store.getOrCreate("example.package")
        repeat(2_000) { index ->
            preferences.volume = (index + 1) / 2_000f
            store.save()
        }

        val persistedVolume = withTimeout(30_000) {
            dataStore.data
                .mapNotNull(::readFirstVolume)
                .debounce(1_000)
                .first()
        }

        assertEquals(1f, persistedVolume, 0f)
    }

    private fun createDataStore(fileName: String): DataStore<Preferences> {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        dataStoreScopes += scope
        return PreferenceDataStoreFactory.create(scope = scope) {
            File(temporaryFolder.root, fileName)
        }
    }

    private fun readFirstVolume(preferences: Preferences): Float? {
        val serialized = preferences[AppsKey] ?: return null
        val values = Json.parseToJsonElement(serialized)
            .jsonObject.getValue("values")
            .jsonArray
        val first = values.firstOrNull() ?: return null
        return first.jsonObject["volume"]?.jsonPrimitive?.float ?: 1f
    }

    private companion object {
        val AppsKey = stringPreferencesKey("apps")
        const val EmptyStateJson = """{"values":[],"indices":{}}"""
        const val ExistingStateJson = """
            {
                "values":[{
                    "isPlayer":true,
                    "volume":0.42,
                    "hidden":true,
                    "disableVolumeButtons":true
                }],
                "indices":{"example.package":0},
                "systemSliderVisibility":{"media":false}
            }
        """
    }

    private class ControlledDataStore : DataStore<Preferences> {
        private val state = MutableStateFlow<Preferences>(
            emptyPreferences()
        )
        private val updateMutex = Mutex()
        private var updateCount = 0

        val firstUpdateTransformed = CompletableDeferred<Unit>()
        val allowFirstEmission = CompletableDeferred<Unit>()
        val firstEmissionPublished = CompletableDeferred<Unit>()
        val allowFirstUpdateToFinish = CompletableDeferred<Unit>()
        val secondUpdateFinished = CompletableDeferred<Unit>()

        val currentPreferences: Preferences
            get() = state.value

        override val data: Flow<Preferences> = state

        override suspend fun updateData(
            transform: suspend (t: Preferences) -> Preferences
        ): Preferences = updateMutex.withLock {
            val index = updateCount++
            val updated = transform(state.value)
            if (index == 0) {
                firstUpdateTransformed.complete(Unit)
                allowFirstEmission.await()
                state.value = updated
                firstEmissionPublished.complete(Unit)
                allowFirstUpdateToFinish.await()
            } else {
                state.value = updated
                secondUpdateFinished.complete(Unit)
            }
            updated
        }
    }

    private open class InMemoryDataStore(
        initial: Preferences = emptyPreferences()
    ) : DataStore<Preferences> {
        protected val state = MutableStateFlow(initial)
        private val updateMutex = Mutex()

        val currentPreferences: Preferences
            get() = state.value

        override open val data: Flow<Preferences> = state

        override suspend fun updateData(
            transform: suspend (t: Preferences) -> Preferences
        ): Preferences = updateMutex.withLock {
            transform(state.value).also { state.value = it }
        }
    }

    private class FailFirstUpdateDataStore : InMemoryDataStore() {
        private var shouldFail = true

        val firstFailureObserved = CompletableDeferred<Unit>()
        val successfulUpdateObserved = CompletableDeferred<Unit>()

        override suspend fun updateData(
            transform: suspend (t: Preferences) -> Preferences
        ): Preferences {
            if (shouldFail) {
                shouldFail = false
                firstFailureObserved.complete(Unit)
                throw IOException("Expected test failure")
            }

            return super.updateData(transform).also {
                successfulUpdateObserved.complete(Unit)
            }
        }
    }

    private class FailFirstLoadDataStore : InMemoryDataStore() {
        private val loadAttempts = AtomicInteger()

        val firstFailureObserved = CompletableDeferred<Unit>()

        override val data: Flow<Preferences> = flow {
            if (loadAttempts.getAndIncrement() == 0) {
                firstFailureObserved.complete(Unit)
                throw IOException("Expected test failure")
            }
            emitAll(state)
        }
    }
}
